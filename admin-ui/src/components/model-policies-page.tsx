import { useEffect, useMemo, useState } from 'react'
import { AlertCircle, Plus, Save, Trash2 } from 'lucide-react'
import { toast } from 'sonner'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import {
  AccountTypeInput,
  ModelSelector,
  collectAccountTypeSuggestions,
  nextDerivedAccountType,
} from '@/components/model-policy-controls'
import {
  useCredentials,
  useModelCapabilitiesConfig,
  useModelCatalog,
  useSetModelCapabilitiesConfig,
} from '@/hooks/use-credentials'
import { extractErrorMessage } from '@/lib/utils'
import type {
  AccountTypeDispatchPolicy,
  ModelSupportPolicy,
  StandardAccountTypePreset,
} from '@/types/api'

interface AccountTypePolicyRow {
  id: string
  accountType: string
  allowedModels: string[]
  blockedModels: string[]
}

interface AccountTypeDispatchPolicyRow {
  id: string
  accountType: string
  maxConcurrency: string
  rateLimitBucketCapacity: string
  rateLimitRefillPerSecond: string
}

function createRowId(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
}

function createPolicyRow(
  accountType = '',
  policy?: ModelSupportPolicy
): AccountTypePolicyRow {
  return {
    id: createRowId('policy'),
    accountType,
    allowedModels: policy?.allowedModels ?? [],
    blockedModels: policy?.blockedModels ?? [],
  }
}

function createDispatchPolicyRow(
  accountType = '',
  policy?: AccountTypeDispatchPolicy
): AccountTypeDispatchPolicyRow {
  return {
    id: createRowId('dispatch'),
    accountType,
    maxConcurrency:
      typeof policy?.maxConcurrency === 'number' ? String(policy.maxConcurrency) : '',
    rateLimitBucketCapacity:
      typeof policy?.rateLimitBucketCapacity === 'number'
        ? String(policy.rateLimitBucketCapacity)
        : '',
    rateLimitRefillPerSecond:
      typeof policy?.rateLimitRefillPerSecond === 'number'
        ? String(policy.rateLimitRefillPerSecond)
        : '',
  }
}

function rowsFromPolicies(
  accountTypePolicies: Record<string, ModelSupportPolicy> | undefined
): AccountTypePolicyRow[] {
  return Object.entries(accountTypePolicies ?? {})
    .sort(([left], [right]) => left.localeCompare(right, 'zh-CN'))
    .map(([accountType, policy]) => createPolicyRow(accountType, policy))
}

function rowsFromDispatchPolicies(
  accountTypeDispatchPolicies: Record<string, AccountTypeDispatchPolicy> | undefined
): AccountTypeDispatchPolicyRow[] {
  return Object.entries(accountTypeDispatchPolicies ?? {})
    .sort(([left], [right]) => left.localeCompare(right, 'zh-CN'))
    .map(([accountType, policy]) => createDispatchPolicyRow(accountType, policy))
}

function policiesFromRows(rows: AccountTypePolicyRow[]): Record<string, ModelSupportPolicy> {
  const policies: Record<string, ModelSupportPolicy> = {}

  rows.forEach((row, index) => {
    const accountType = row.accountType.trim()
    const allowedModels = Array.from(
      new Set(row.allowedModels.map((value) => value.trim()).filter(Boolean))
    )
    const blockedModels = Array.from(
      new Set(row.blockedModels.map((value) => value.trim()).filter(Boolean))
    )
    const hasModels = allowedModels.length > 0 || blockedModels.length > 0

    if (!accountType && !hasModels) {
      return
    }
    if (!accountType) {
      throw new Error(`模型策略第 ${index + 1} 行缺少账号类型`)
    }
    if (!hasModels) {
      throw new Error(`模型策略第 ${index + 1} 行至少需要配置一项允许或禁用模型`)
    }

    const normalizedAccountType = accountType.toLowerCase()
    if (policies[normalizedAccountType]) {
      throw new Error(`账号类型 ${normalizedAccountType} 在模型策略中重复配置`)
    }

    policies[normalizedAccountType] = {
      allowedModels,
      blockedModels,
    }
  })

  return policies
}

function dispatchPoliciesFromRows(
  rows: AccountTypeDispatchPolicyRow[]
): Record<string, AccountTypeDispatchPolicy> {
  const policies: Record<string, AccountTypeDispatchPolicy> = {}

  rows.forEach((row, index) => {
    const accountType = row.accountType.trim()
    const trimmedMaxConcurrency = row.maxConcurrency.trim()
    const trimmedBucketCapacity = row.rateLimitBucketCapacity.trim()
    const trimmedRefillPerSecond = row.rateLimitRefillPerSecond.trim()
    const hasAnyDispatchSetting =
      trimmedMaxConcurrency !== '' ||
      trimmedBucketCapacity !== '' ||
      trimmedRefillPerSecond !== ''

    if (!accountType && !hasAnyDispatchSetting) {
      return
    }
    if (!accountType) {
      throw new Error(`调度策略第 ${index + 1} 行缺少账号类型`)
    }
    if (!hasAnyDispatchSetting) {
      throw new Error(`调度策略第 ${index + 1} 行至少需要配置一项调度参数`)
    }

    const normalizedAccountType = accountType.toLowerCase()
    if (policies[normalizedAccountType]) {
      throw new Error(`账号类型 ${normalizedAccountType} 在调度策略中重复配置`)
    }

    let maxConcurrency: number | undefined
    if (trimmedMaxConcurrency !== '') {
      const parsed = Number.parseInt(trimmedMaxConcurrency, 10)
      if (!Number.isInteger(parsed) || parsed <= 0) {
        throw new Error(`调度策略第 ${index + 1} 行的并发上限必须是大于 0 的整数`)
      }
      maxConcurrency = parsed
    }

    let rateLimitBucketCapacity: number | undefined
    if (trimmedBucketCapacity !== '') {
      const parsed = Number.parseFloat(trimmedBucketCapacity)
      if (!Number.isFinite(parsed) || parsed < 0) {
        throw new Error(`调度策略第 ${index + 1} 行的 Bucket 容量必须是大于等于 0 的数字`)
      }
      rateLimitBucketCapacity = parsed
    }

    let rateLimitRefillPerSecond: number | undefined
    if (trimmedRefillPerSecond !== '') {
      const parsed = Number.parseFloat(trimmedRefillPerSecond)
      if (!Number.isFinite(parsed) || parsed < 0) {
        throw new Error(`调度策略第 ${index + 1} 行的回填速率必须是大于等于 0 的数字`)
      }
      rateLimitRefillPerSecond = parsed
    }

    policies[normalizedAccountType] = {
      maxConcurrency,
      rateLimitBucketCapacity,
      rateLimitRefillPerSecond,
    }
  })

  return policies
}

function summarizeModelValues(values: string[], modelLabelMap: Map<string, string>): string {
  if (values.length === 0) {
    return '未设置'
  }

  const labels = values.slice(0, 2).map((value) => modelLabelMap.get(value) ?? value)
  if (values.length <= 2) {
    return labels.join('、')
  }

  return `${labels.join('、')} 等 ${values.length} 项`
}

function summarizeRecommendedPolicy(
  preset: StandardAccountTypePreset,
  modelLabelMap: Map<string, string>
): string {
  const recommendedPolicy = preset.recommendedPolicy
  if (!recommendedPolicy) {
    return '默认不附加模型限制，适合作为标准主力类型或衍生类型基线。'
  }

  const allowedModels = recommendedPolicy.allowedModels ?? []
  const blockedModels = recommendedPolicy.blockedModels ?? []

  return [
    `允许：${summarizeModelValues(allowedModels, modelLabelMap)}`,
    `禁用：${summarizeModelValues(blockedModels, modelLabelMap)}`,
  ].join(' / ')
}

function summarizeRecommendedDispatchPolicy(preset: StandardAccountTypePreset): string {
  const recommendedDispatchPolicy = preset.recommendedDispatchPolicy
  if (!recommendedDispatchPolicy) {
    return '默认不附加调度覆盖，跟随全局默认调度。'
  }

  const parts: string[] = []

  if (typeof recommendedDispatchPolicy.maxConcurrency === 'number') {
    parts.push(`并发=${recommendedDispatchPolicy.maxConcurrency}`)
  }
  if (typeof recommendedDispatchPolicy.rateLimitBucketCapacity === 'number') {
    parts.push(
      recommendedDispatchPolicy.rateLimitBucketCapacity === 0
        ? 'Bucket=禁用'
        : `Bucket=${recommendedDispatchPolicy.rateLimitBucketCapacity}`
    )
  }
  if (typeof recommendedDispatchPolicy.rateLimitRefillPerSecond === 'number') {
    parts.push(
      recommendedDispatchPolicy.rateLimitRefillPerSecond === 0
        ? '回填=禁用'
        : `回填=${recommendedDispatchPolicy.rateLimitRefillPerSecond} token/s`
    )
  }

  return parts.length > 0 ? parts.join(' / ') : '默认不附加调度覆盖，跟随全局默认调度。'
}

export function ModelPoliciesPage() {
  const { data: credentialsData } = useCredentials()
  const {
    data: modelCapabilitiesData,
    isLoading: isLoadingCapabilities,
  } = useModelCapabilitiesConfig()
  const { data: modelCatalogData } = useModelCatalog()
  const { mutate: setModelCapabilitiesConfig, isPending: isSettingCapabilities } =
    useSetModelCapabilitiesConfig()

  const [policyRows, setPolicyRows] = useState<AccountTypePolicyRow[]>([])
  const [dispatchPolicyRows, setDispatchPolicyRows] = useState<AccountTypeDispatchPolicyRow[]>([])
  const [modelCapabilitiesJson, setModelCapabilitiesJson] = useState('{\n  \n}')
  const [dispatchPoliciesJson, setDispatchPoliciesJson] = useState('{\n  \n}')

  const modelCatalog = modelCatalogData?.models ?? []
  const standardAccountTypePresets = modelCapabilitiesData?.standardAccountTypePresets ?? []
  const modelLabelMap = useMemo(
    () => new Map(modelCatalog.map((model) => [model.policyId, model.displayName] as const)),
    [modelCatalog]
  )
  const accountTypeSuggestions = useMemo(() => {
    const values = new Set(
      collectAccountTypeSuggestions(
        credentialsData?.credentials,
        modelCapabilitiesData?.accountTypePolicies,
        modelCapabilitiesData?.accountTypeDispatchPolicies,
        standardAccountTypePresets
      )
    )

    for (const row of policyRows) {
      const accountType = row.accountType.trim()
      if (accountType) {
        values.add(accountType)
      }
    }

    for (const row of dispatchPolicyRows) {
      const accountType = row.accountType.trim()
      if (accountType) {
        values.add(accountType)
      }
    }

    return Array.from(values).sort((left, right) => left.localeCompare(right, 'zh-CN'))
  }, [
    credentialsData?.credentials,
    dispatchPolicyRows,
    modelCapabilitiesData?.accountTypeDispatchPolicies,
    modelCapabilitiesData?.accountTypePolicies,
    policyRows,
    standardAccountTypePresets,
  ])

  useEffect(() => {
    if (!modelCapabilitiesData) {
      return
    }

    setPolicyRows(rowsFromPolicies(modelCapabilitiesData.accountTypePolicies))
    setDispatchPolicyRows(rowsFromDispatchPolicies(modelCapabilitiesData.accountTypeDispatchPolicies))
    setModelCapabilitiesJson(
      JSON.stringify(modelCapabilitiesData.accountTypePolicies ?? {}, null, 2)
    )
    setDispatchPoliciesJson(
      JSON.stringify(modelCapabilitiesData.accountTypeDispatchPolicies ?? {}, null, 2)
    )
  }, [modelCapabilitiesData])

  const handleSaveModelCapabilitiesVisual = () => {
    let parsed: Record<string, ModelSupportPolicy>
    try {
      parsed = policiesFromRows(policyRows)
    } catch (error) {
      toast.error(extractErrorMessage(error))
      return
    }

    setModelCapabilitiesConfig(
      { accountTypePolicies: parsed },
      {
        onSuccess: (response) => {
          setPolicyRows(rowsFromPolicies(response.accountTypePolicies))
          setModelCapabilitiesJson(JSON.stringify(response.accountTypePolicies ?? {}, null, 2))
          toast.success('账号类型模型策略已更新')
        },
        onError: (error) => {
          toast.error(`保存失败: ${extractErrorMessage(error)}`)
        },
      }
    )
  }

  const handleSaveModelCapabilitiesJson = () => {
    let parsed: Record<string, ModelSupportPolicy>
    try {
      const raw = modelCapabilitiesJson.trim() || '{}'
      const value = JSON.parse(raw)
      if (value === null || typeof value !== 'object' || Array.isArray(value)) {
        toast.error('账号类型模型策略必须是一个 JSON 对象')
        return
      }
      parsed = value as Record<string, ModelSupportPolicy>
    } catch (error) {
      toast.error(`账号类型模型策略 JSON 解析失败: ${extractErrorMessage(error)}`)
      return
    }

    setModelCapabilitiesConfig(
      { accountTypePolicies: parsed },
      {
        onSuccess: (response) => {
          setPolicyRows(rowsFromPolicies(response.accountTypePolicies))
          setModelCapabilitiesJson(JSON.stringify(response.accountTypePolicies ?? {}, null, 2))
          toast.success('账号类型模型策略已更新')
        },
        onError: (error) => {
          toast.error(`保存失败: ${extractErrorMessage(error)}`)
        },
      }
    )
  }

  const handleSaveDispatchPoliciesVisual = () => {
    let parsed: Record<string, AccountTypeDispatchPolicy>
    try {
      parsed = dispatchPoliciesFromRows(dispatchPolicyRows)
    } catch (error) {
      toast.error(extractErrorMessage(error))
      return
    }

    setModelCapabilitiesConfig(
      { accountTypeDispatchPolicies: parsed },
      {
        onSuccess: (response) => {
          setDispatchPolicyRows(rowsFromDispatchPolicies(response.accountTypeDispatchPolicies))
          setDispatchPoliciesJson(
            JSON.stringify(response.accountTypeDispatchPolicies ?? {}, null, 2)
          )
          toast.success('账号类型调度策略已更新')
        },
        onError: (error) => {
          toast.error(`保存失败: ${extractErrorMessage(error)}`)
        },
      }
    )
  }

  const handleSaveDispatchPoliciesJson = () => {
    let parsed: Record<string, AccountTypeDispatchPolicy>
    try {
      const raw = dispatchPoliciesJson.trim() || '{}'
      const value = JSON.parse(raw)
      if (value === null || typeof value !== 'object' || Array.isArray(value)) {
        toast.error('账号类型调度策略必须是一个 JSON 对象')
        return
      }
      parsed = value as Record<string, AccountTypeDispatchPolicy>
    } catch (error) {
      toast.error(`账号类型调度策略 JSON 解析失败: ${extractErrorMessage(error)}`)
      return
    }

    setModelCapabilitiesConfig(
      { accountTypeDispatchPolicies: parsed },
      {
        onSuccess: (response) => {
          setDispatchPolicyRows(rowsFromDispatchPolicies(response.accountTypeDispatchPolicies))
          setDispatchPoliciesJson(
            JSON.stringify(response.accountTypeDispatchPolicies ?? {}, null, 2)
          )
          toast.success('账号类型调度策略已更新')
        },
        onError: (error) => {
          toast.error(`保存失败: ${extractErrorMessage(error)}`)
        },
      }
    )
  }

  const appendModelPresetRow = (preset: StandardAccountTypePreset, derived: boolean) => {
    if (!derived && !preset.recommendedPolicy) {
      toast.error(
        `${preset.displayName} 默认不附加模型限制，可直接在凭据卡片中使用该类型名，或复制为衍生类型后再补充规则`
      )
      return
    }

    const existingAccountTypes = policyRows.map((row) => row.accountType)
    const nextAccountType = derived
      ? nextDerivedAccountType(preset.id, existingAccountTypes)
      : preset.id

    if (!derived && policyRows.some((row) => row.accountType.trim().toLowerCase() === preset.id)) {
      toast.error(`账号类型 ${preset.id} 已存在于模型策略中，可直接编辑现有规则`)
      return
    }

    setPolicyRows((prev) => [
      ...prev,
      createPolicyRow(
        nextAccountType,
        preset.recommendedPolicy ?? { allowedModels: [], blockedModels: [] }
      ),
    ])
  }

  const appendDispatchPresetRow = (preset: StandardAccountTypePreset, derived: boolean) => {
    if (!derived && !preset.recommendedDispatchPolicy) {
      toast.error(
        `${preset.displayName} 默认不附加调度覆盖，可继续跟随全局参数，或复制为衍生类型后手动补充`
      )
      return
    }

    const existingAccountTypes = dispatchPolicyRows.map((row) => row.accountType)
    const nextAccountType = derived
      ? nextDerivedAccountType(preset.id, existingAccountTypes)
      : preset.id

    if (
      !derived &&
      dispatchPolicyRows.some((row) => row.accountType.trim().toLowerCase() === preset.id)
    ) {
      toast.error(`账号类型 ${preset.id} 已存在于调度策略中，可直接编辑现有规则`)
      return
    }

    setDispatchPolicyRows((prev) => [
      ...prev,
      createDispatchPolicyRow(nextAccountType, preset.recommendedDispatchPolicy ?? {}),
    ])
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-2">
        <h2 className="text-2xl font-semibold tracking-tight">账号类型策略</h2>
        <p className="text-muted-foreground">
          统一维护账号类型的默认模型能力和默认调度能力，并把单账号覆盖保留为少量例外。
        </p>
      </div>

      <Card className="border-muted shadow-sm">
        <CardHeader>
          <CardTitle>策略优先级</CardTitle>
          <CardDescription>
            推荐把稳定的账号池差异放到账号类型策略里，把临时例外保留在凭据卡片中单独覆盖。
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="flex max-w-3xl gap-2 text-sm text-muted-foreground">
            <AlertCircle className="h-5 w-5 shrink-0 text-yellow-600 dark:text-yellow-500" />
            <p>
              模型能力的生效顺序是“账号类型默认策略”先命中，再叠加“单账号允许/禁用模型”，最后再考虑运行时临时限制。
              调度能力的生效顺序是“凭据级并发 / bucket 覆盖”优先，其次是“账号类型调度策略”，最后才回退到全局默认值。
            </p>
          </div>
        </CardContent>
      </Card>

      <Card className="border-muted shadow-sm">
        <CardHeader>
          <CardTitle>标准账号类型预设</CardTitle>
          <CardDescription>
            先复用系统内置标准类型，再按业务需要复制出 `power-custom`、`pro-plus-canary` 这类衍生类型。
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="text-sm text-muted-foreground">
            标准类型用于统一账号池基线；衍生类型用于灰度、金丝雀、渠道隔离等场景，复制后可继续编辑模型限制和调度参数。
          </div>
          <div className="grid gap-4 xl:grid-cols-2">
            {standardAccountTypePresets.map((preset) => {
              const subscriptionTitleExamples = preset.subscriptionTitleExamples ?? []

              return (
                <div key={preset.id} className="space-y-3 rounded-lg border bg-muted/20 p-4">
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant="secondary">{preset.displayName}</Badge>
                    <Badge variant="outline">{preset.id}</Badge>
                    {preset.recommendedPolicy || preset.recommendedDispatchPolicy ? (
                      <Badge variant="outline">含推荐基线</Badge>
                    ) : (
                      <Badge variant="outline">无默认覆盖</Badge>
                    )}
                  </div>
                  <div className="space-y-1">
                    <p className="text-sm">{preset.description}</p>
                    {subscriptionTitleExamples.length > 0 && (
                      <p className="text-xs text-muted-foreground">
                        识别示例：{subscriptionTitleExamples.join('、')}
                      </p>
                    )}
                    <p className="text-xs text-muted-foreground">
                      模型推荐：{summarizeRecommendedPolicy(preset, modelLabelMap)}
                    </p>
                    <p className="text-xs text-muted-foreground">
                      调度推荐：{summarizeRecommendedDispatchPolicy(preset)}
                    </p>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {preset.recommendedPolicy && (
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() => appendModelPresetRow(preset, false)}
                      >
                        复制模型基线
                      </Button>
                    )}
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() => appendModelPresetRow(preset, true)}
                    >
                      复制模型衍生类型
                    </Button>
                    {preset.recommendedDispatchPolicy && (
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() => appendDispatchPresetRow(preset, false)}
                      >
                        复制调度基线
                      </Button>
                    )}
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() => appendDispatchPresetRow(preset, true)}
                    >
                      复制调度衍生类型
                    </Button>
                  </div>
                </div>
              )
            })}
          </div>
        </CardContent>
      </Card>

      <Card className="border-muted shadow-sm">
        <CardHeader>
          <CardTitle>账号类型模型策略</CardTitle>
          <CardDescription>
            维护“账号类型 → 默认允许/禁用模型”映射，减少凭据卡片上的重复手工配置。
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="text-sm text-muted-foreground">
              账号类型优先复用已有类型；确实需要时再新建自定义类型。
            </div>
            <div className="flex items-center gap-2">
              {isLoadingCapabilities && <Badge variant="outline">加载中</Badge>}
              <Button
                type="button"
                variant="outline"
                onClick={() => setPolicyRows((prev) => [...prev, createPolicyRow()])}
              >
                <Plus className="mr-2 h-4 w-4" />
                新增模型策略
              </Button>
            </div>
          </div>

          {policyRows.length === 0 ? (
            <div className="rounded-lg border border-dashed p-6 text-sm text-muted-foreground">
              还没有配置账号类型模型策略。可以先新增一条规则，再为该类型选择允许/禁用模型。
            </div>
          ) : (
            <div className="space-y-4">
              {policyRows.map((row, index) => (
                <div key={row.id} className="space-y-4 rounded-lg border bg-muted/20 p-4">
                  <div className="flex items-center justify-between gap-3">
                    <Badge variant="secondary">模型策略 {index + 1}</Badge>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        setPolicyRows((prev) => prev.filter((item) => item.id !== row.id))
                      }
                    >
                      <Trash2 className="mr-2 h-4 w-4" />
                      删除
                    </Button>
                  </div>

                  <AccountTypeInput
                    label="账号类型"
                    value={row.accountType}
                    onChange={(value) =>
                      setPolicyRows((prev) =>
                        prev.map((item) =>
                          item.id === row.id ? { ...item, accountType: value } : item
                        )
                      )
                    }
                    suggestions={accountTypeSuggestions}
                    standardAccountTypePresets={standardAccountTypePresets}
                    placeholder="输入新的账号类型，例如 reseller-a"
                    description="优先复用内置标准类型；若要灰度或隔离流量，建议从标准类型复制出 `power-custom` 这类衍生类型。保存时会自动标准化为小写。"
                  />

                  <div className="grid gap-4 xl:grid-cols-2">
                    <ModelSelector
                      label="默认允许模型"
                      selectedValues={row.allowedModels}
                      onChange={(values) =>
                        setPolicyRows((prev) =>
                          prev.map((item) =>
                            item.id === row.id ? { ...item, allowedModels: values } : item
                          )
                        )
                      }
                      options={modelCatalog}
                      description="留空表示该账号类型不额外限制允许列表。"
                    />

                    <ModelSelector
                      label="默认禁用模型"
                      selectedValues={row.blockedModels}
                      onChange={(values) =>
                        setPolicyRows((prev) =>
                          prev.map((item) =>
                            item.id === row.id ? { ...item, blockedModels: values } : item
                          )
                        )
                      }
                      options={modelCatalog}
                      description="拒绝列表始终优先，适合表达“这一类账号明确不支持某模型”。"
                    />
                  </div>
                </div>
              ))}
            </div>
          )}

          <div className="flex items-center justify-between gap-4">
            <p className="text-sm text-muted-foreground">
              这里适合维护长期稳定规则；具体账号的临时例外仍在凭据卡片里处理。
            </p>
            <Button onClick={handleSaveModelCapabilitiesVisual} disabled={isSettingCapabilities}>
              <Save className="mr-2 h-4 w-4" />
              保存模型策略
            </Button>
          </div>

          <details className="rounded-lg border border-dashed bg-muted/20 p-4">
            <summary className="cursor-pointer text-sm font-medium">高级模式 / 模型策略 JSON</summary>
            <div className="mt-4 space-y-4">
              <textarea
                rows={14}
                value={modelCapabilitiesJson}
                onChange={(e) => setModelCapabilitiesJson(e.target.value)}
                spellCheck={false}
                className="flex min-h-[280px] w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                placeholder={`{\n  "power": {\n    "allowedModels": ["claude-opus-4.6"],\n    "blockedModels": ["claude-opus-4.7"]\n  }\n}`}
              />
              <p className="text-xs text-muted-foreground">
                适合批量粘贴或紧急修正；保存成功后，可视化表单会自动同步最新结果。
              </p>
              <div className="flex justify-end">
                <Button
                  variant="outline"
                  onClick={handleSaveModelCapabilitiesJson}
                  disabled={isSettingCapabilities}
                >
                  <Save className="mr-2 h-4 w-4" />
                  保存模型策略 JSON
                </Button>
              </div>
            </div>
          </details>
        </CardContent>
      </Card>

      <Card className="border-muted shadow-sm">
        <CardHeader>
          <CardTitle>账号类型调度策略</CardTitle>
          <CardDescription>
            维护“账号类型 → 默认并发 / bucket 覆盖”映射，让同类账号自动套用统一承载参数。
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="text-sm text-muted-foreground">
              优先在这里表达稳定的承载能力差异；凭据卡片里的并发和 bucket 覆盖只处理临时例外。
            </div>
            <Button
              type="button"
              variant="outline"
              onClick={() => setDispatchPolicyRows((prev) => [...prev, createDispatchPolicyRow()])}
            >
              <Plus className="mr-2 h-4 w-4" />
              新增调度策略
            </Button>
          </div>

          {dispatchPolicyRows.length === 0 ? (
            <div className="rounded-lg border border-dashed p-6 text-sm text-muted-foreground">
              还没有配置账号类型调度策略。可以先新增一条规则，为该类型设置统一的并发和 bucket 参数。
            </div>
          ) : (
            <div className="space-y-4">
              {dispatchPolicyRows.map((row, index) => (
                <div key={row.id} className="space-y-4 rounded-lg border bg-muted/20 p-4">
                  <div className="flex items-center justify-between gap-3">
                    <Badge variant="secondary">调度策略 {index + 1}</Badge>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        setDispatchPolicyRows((prev) => prev.filter((item) => item.id !== row.id))
                      }
                    >
                      <Trash2 className="mr-2 h-4 w-4" />
                      删除
                    </Button>
                  </div>

                  <AccountTypeInput
                    label="账号类型"
                    value={row.accountType}
                    onChange={(value) =>
                      setDispatchPolicyRows((prev) =>
                        prev.map((item) =>
                          item.id === row.id ? { ...item, accountType: value } : item
                        )
                      )
                    }
                    suggestions={accountTypeSuggestions}
                    standardAccountTypePresets={standardAccountTypePresets}
                    placeholder="输入新的账号类型，例如 power-canary"
                    description="留空表示不创建该行；保存时会自动标准化为小写。建议标准类型和衍生类型分别维护稳定的调度承载差异。"
                  />

                  <div className="grid gap-4 md:grid-cols-3">
                    <div className="space-y-2">
                      <label className="text-sm font-medium">默认并发上限</label>
                      <Input
                        type="number"
                        min="1"
                        step="1"
                        value={row.maxConcurrency}
                        onChange={(e) =>
                          setDispatchPolicyRows((prev) =>
                            prev.map((item) =>
                              item.id === row.id
                                ? { ...item, maxConcurrency: e.target.value }
                                : item
                            )
                          )
                        }
                        placeholder="留空表示跟随全局"
                      />
                      <p className="text-xs text-muted-foreground">
                        只接受大于 0 的整数；留空表示继续跟随全局默认并发。
                      </p>
                    </div>

                    <div className="space-y-2">
                      <label className="text-sm font-medium">Bucket 容量覆盖</label>
                      <Input
                        type="number"
                        min="0"
                        step="0.1"
                        value={row.rateLimitBucketCapacity}
                        onChange={(e) =>
                          setDispatchPolicyRows((prev) =>
                            prev.map((item) =>
                              item.id === row.id
                                ? { ...item, rateLimitBucketCapacity: e.target.value }
                                : item
                            )
                          )
                        }
                        placeholder="留空表示跟随全局"
                      />
                      <p className="text-xs text-muted-foreground">
                        `0` 表示对该类型禁用 bucket；留空表示继续跟随全局。
                      </p>
                    </div>

                    <div className="space-y-2">
                      <label className="text-sm font-medium">回填速率覆盖</label>
                      <Input
                        type="number"
                        min="0"
                        step="0.1"
                        value={row.rateLimitRefillPerSecond}
                        onChange={(e) =>
                          setDispatchPolicyRows((prev) =>
                            prev.map((item) =>
                              item.id === row.id
                                ? { ...item, rateLimitRefillPerSecond: e.target.value }
                                : item
                            )
                          )
                        }
                        placeholder="留空表示跟随全局"
                      />
                      <p className="text-xs text-muted-foreground">
                        `0` 表示对该类型禁用 bucket；留空表示继续跟随全局。
                      </p>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}

          <div className="flex items-center justify-between gap-4">
            <p className="text-sm text-muted-foreground">
              这里适合沉淀长期稳定的承载参数，例如 `power` 类型固定更高并发、关闭本地 bucket 覆盖。
            </p>
            <Button onClick={handleSaveDispatchPoliciesVisual} disabled={isSettingCapabilities}>
              <Save className="mr-2 h-4 w-4" />
              保存调度策略
            </Button>
          </div>

          <details className="rounded-lg border border-dashed bg-muted/20 p-4">
            <summary className="cursor-pointer text-sm font-medium">高级模式 / 调度策略 JSON</summary>
            <div className="mt-4 space-y-4">
              <textarea
                rows={14}
                value={dispatchPoliciesJson}
                onChange={(e) => setDispatchPoliciesJson(e.target.value)}
                spellCheck={false}
                className="flex min-h-[280px] w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                placeholder={`{\n  "power": {\n    "maxConcurrency": 20,\n    "rateLimitBucketCapacity": 0,\n    "rateLimitRefillPerSecond": 0\n  }\n}`}
              />
              <p className="text-xs text-muted-foreground">
                适合批量粘贴或紧急修正；保存成功后，可视化表单会自动同步最新结果。
              </p>
              <div className="flex justify-end">
                <Button
                  variant="outline"
                  onClick={handleSaveDispatchPoliciesJson}
                  disabled={isSettingCapabilities}
                >
                  <Save className="mr-2 h-4 w-4" />
                  保存调度策略 JSON
                </Button>
              </div>
            </div>
          </details>
        </CardContent>
      </Card>
    </div>
  )
}
