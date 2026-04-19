import { useEffect, useMemo, useState } from 'react'
import { AlertCircle, Plus, Save, Trash2 } from 'lucide-react'
import { toast } from 'sonner'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
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
import type { ModelSupportPolicy, StandardAccountTypePreset } from '@/types/api'

interface AccountTypePolicyRow {
  id: string
  accountType: string
  allowedModels: string[]
  blockedModels: string[]
}

function createPolicyRow(
  accountType = '',
  policy?: ModelSupportPolicy
): AccountTypePolicyRow {
  return {
    id: `policy-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    accountType,
    allowedModels: policy?.allowedModels ?? [],
    blockedModels: policy?.blockedModels ?? [],
  }
}

function rowsFromPolicies(
  accountTypePolicies: Record<string, ModelSupportPolicy> | undefined
): AccountTypePolicyRow[] {
  return Object.entries(accountTypePolicies ?? {})
    .sort(([left], [right]) => left.localeCompare(right, 'zh-CN'))
    .map(([accountType, policy]) => createPolicyRow(accountType, policy))
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
      throw new Error(`第 ${index + 1} 行缺少账号类型`)
    }
    if (!hasModels) {
      throw new Error(`第 ${index + 1} 行至少需要配置一项允许或禁用模型`)
    }

    const normalizedAccountType = accountType.toLowerCase()
    if (policies[normalizedAccountType]) {
      throw new Error(`账号类型 ${normalizedAccountType} 重复配置`)
    }

    policies[normalizedAccountType] = {
      allowedModels,
      blockedModels,
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

  return [
    `允许：${summarizeModelValues(recommendedPolicy.allowedModels, modelLabelMap)}`,
    `禁用：${summarizeModelValues(recommendedPolicy.blockedModels, modelLabelMap)}`,
  ].join(' / ')
}

export function ModelPoliciesPage() {
  const { data: credentialsData } = useCredentials()
  const { data: modelCapabilitiesData, isLoading: isLoadingCapabilities } = useModelCapabilitiesConfig()
  const { data: modelCatalogData } = useModelCatalog()
  const { mutate: setModelCapabilitiesConfig, isPending: isSettingCapabilities } =
    useSetModelCapabilitiesConfig()

  const [policyRows, setPolicyRows] = useState<AccountTypePolicyRow[]>([])
  const [modelCapabilitiesJson, setModelCapabilitiesJson] = useState('{\n  \n}')

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
        standardAccountTypePresets
      )
    )
    for (const row of policyRows) {
      const accountType = row.accountType.trim()
      if (accountType) {
        values.add(accountType)
      }
    }
    return Array.from(values).sort((left, right) => left.localeCompare(right, 'zh-CN'))
  }, [
    credentialsData?.credentials,
    modelCapabilitiesData?.accountTypePolicies,
    policyRows,
    standardAccountTypePresets,
  ])

  useEffect(() => {
    if (!modelCapabilitiesData) {
      return
    }
    setPolicyRows(rowsFromPolicies(modelCapabilitiesData.accountTypePolicies))
    setModelCapabilitiesJson(
      JSON.stringify(modelCapabilitiesData.accountTypePolicies ?? {}, null, 2)
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
        toast.error('账号类型策略必须是一个 JSON 对象')
        return
      }
      parsed = value as Record<string, ModelSupportPolicy>
    } catch (error) {
      toast.error(`账号类型策略 JSON 解析失败: ${extractErrorMessage(error)}`)
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

  const appendPresetRow = (preset: StandardAccountTypePreset, derived: boolean) => {
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
      toast.error(`账号类型 ${preset.id} 已存在，可直接编辑现有规则或复制为衍生类型`)
      return
    }

    setPolicyRows((prev) => [
      ...prev,
      createPolicyRow(nextAccountType, preset.recommendedPolicy ?? { allowedModels: [], blockedModels: [] }),
    ])
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-2">
        <h2 className="text-2xl font-semibold tracking-tight">模型策略</h2>
        <p className="text-muted-foreground">
          统一维护账号类型默认模型能力，并保留单账号覆盖作为例外处理。
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
          <div className="flex text-sm text-muted-foreground max-w-3xl gap-2">
            <AlertCircle className="h-5 w-5 shrink-0 text-yellow-600 dark:text-yellow-500" />
            <p>
              生效顺序是“账号类型默认策略”先命中，再叠加“单账号允许/禁用模型”，最后再考虑运行时临时限制。
              显式禁用始终优先，适合表达“这一类账号明确不支持某模型”。
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
            标准类型用于统一账号池基线；衍生类型用于灰度、金丝雀、渠道隔离等场景，复制后可继续编辑模型限制。
          </div>
          <div className="grid gap-4 xl:grid-cols-2">
            {standardAccountTypePresets.map((preset) => (
              <div key={preset.id} className="space-y-3 rounded-lg border bg-muted/20 p-4">
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant="secondary">{preset.displayName}</Badge>
                  <Badge variant="outline">{preset.id}</Badge>
                  {preset.recommendedPolicy ? (
                    <Badge variant="outline">含推荐基线</Badge>
                  ) : (
                    <Badge variant="outline">无默认限制</Badge>
                  )}
                </div>
                <div className="space-y-1">
                  <p className="text-sm">{preset.description}</p>
                  {preset.subscriptionTitleExamples.length > 0 && (
                    <p className="text-xs text-muted-foreground">
                      识别示例：{preset.subscriptionTitleExamples.join('、')}
                    </p>
                  )}
                  <p className="text-xs text-muted-foreground">
                    推荐策略：{summarizeRecommendedPolicy(preset, modelLabelMap)}
                  </p>
                </div>
                <div className="flex flex-wrap gap-2">
                  {preset.recommendedPolicy && (
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() => appendPresetRow(preset, false)}
                    >
                      复制为标准类型
                    </Button>
                  )}
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={() => appendPresetRow(preset, true)}
                  >
                    复制为衍生类型
                  </Button>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card className="border-muted shadow-sm">
        <CardHeader>
          <CardTitle>账号类型模型策略</CardTitle>
          <CardDescription>
            优先通过选择维护“账号类型 → 默认允许/禁用模型”映射，减少手工输入错误。
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
                新增账号类型策略
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
                    <Badge variant="secondary">策略 {index + 1}</Badge>
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
              <Save className="h-4 w-4 mr-2" />
              保存账号类型策略
            </Button>
          </div>

          <details className="rounded-lg border border-dashed bg-muted/20 p-4">
            <summary className="cursor-pointer text-sm font-medium">高级模式 / 原始 JSON</summary>
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
                  <Save className="h-4 w-4 mr-2" />
                  保存原始 JSON
                </Button>
              </div>
            </div>
          </details>
        </CardContent>
      </Card>
    </div>
  )
}
