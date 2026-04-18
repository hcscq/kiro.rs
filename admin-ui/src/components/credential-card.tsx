import { useState, type ReactNode } from 'react'
import { toast } from 'sonner'
import { RefreshCw, ChevronUp, ChevronDown, Wallet, Trash2, Loader2 } from 'lucide-react'
import {
  AccountTypeInput,
  ModelSelector,
} from '@/components/model-policy-controls'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { Input } from '@/components/ui/input'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import type { CredentialStatusItem, BalanceResponse, ModelCatalogItem } from '@/types/api'
import {
  useSetDisabled,
  useSetCredentialModelPolicy,
  useSetCredentialRateLimitConfig,
  useSetMaxConcurrency,
  useSetPriority,
  useResetFailure,
  useDeleteCredential,
  useForceRefreshToken,
} from '@/hooks/use-credentials'
import { cn } from '@/lib/utils'

interface CredentialCardProps {
  credential: CredentialStatusItem
  onViewBalance: (id: number) => void
  selected: boolean
  onToggleSelect: () => void
  balance: BalanceResponse | null
  loadingBalance: boolean
  accountTypeSuggestions: string[]
  modelCatalog: ModelCatalogItem[]
}

function formatLastUsed(lastUsedAt: string | null): string {
  if (!lastUsedAt) return '从未使用'
  const date = new Date(lastUsedAt)
  const now = new Date()
  const diff = now.getTime() - date.getTime()
  if (diff < 0) return '刚刚'
  const seconds = Math.floor(diff / 1000)
  if (seconds < 60) return `${seconds} 秒前`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes} 分钟前`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours} 小时前`
  const days = Math.floor(hours / 24)
  return `${days} 天前`
}

function formatRestrictionExpiresAt(expiresAt: string): string {
  const date = new Date(expiresAt)
  if (Number.isNaN(date.getTime())) {
    return expiresAt
  }
  return date.toLocaleString('zh-CN', { hour12: false })
}

function summarizeSelectedModels(
  values: string[],
  modelCatalog: ModelCatalogItem[]
): string {
  if (values.length === 0) {
    return '未设置'
  }

  const displayNameMap = new Map(
    modelCatalog.map((model) => [model.policyId, model.displayName] as const)
  )
  const labels = values.slice(0, 2).map((value) => displayNameMap.get(value) ?? value)

  if (values.length <= 2) {
    return labels.join('、')
  }

  return `${labels.join('、')} 等 ${values.length} 项`
}

function InfoTile({
  label,
  className,
  children,
}: {
  label: string
  className?: string
  children: ReactNode
}) {
  return (
    <div className={cn('rounded-lg border bg-muted/20 px-3 py-2.5', className)}>
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-1 text-sm font-medium leading-snug">{children}</div>
    </div>
  )
}

export function CredentialCard({
  credential,
  onViewBalance,
  selected,
  onToggleSelect,
  balance,
  loadingBalance,
  accountTypeSuggestions,
  modelCatalog,
}: CredentialCardProps) {
  const [editingPriority, setEditingPriority] = useState(false)
  const [priorityValue, setPriorityValue] = useState(String(credential.priority))
  const [editingMaxConcurrency, setEditingMaxConcurrency] = useState(false)
  const [maxConcurrencyValue, setMaxConcurrencyValue] = useState(
    credential.maxConcurrency ? String(credential.maxConcurrency) : ''
  )
  const [editingRateLimitConfig, setEditingRateLimitConfig] = useState(false)
  const [bucketCapacityValue, setBucketCapacityValue] = useState(
    credential.rateLimitBucketCapacityOverride !== undefined &&
    credential.rateLimitBucketCapacityOverride !== null
      ? String(credential.rateLimitBucketCapacityOverride)
      : ''
  )
  const [refillPerSecondValue, setRefillPerSecondValue] = useState(
    credential.rateLimitRefillPerSecondOverride !== undefined &&
    credential.rateLimitRefillPerSecondOverride !== null
      ? String(credential.rateLimitRefillPerSecondOverride)
      : ''
  )
  const [showModelPolicyDialog, setShowModelPolicyDialog] = useState(false)
  const [accountTypeValue, setAccountTypeValue] = useState(credential.accountType ?? '')
  const [allowedModelsValue, setAllowedModelsValue] = useState(credential.allowedModels ?? [])
  const [blockedModelsValue, setBlockedModelsValue] = useState(credential.blockedModels ?? [])
  const [clearRuntimeModelRestrictions, setClearRuntimeModelRestrictions] = useState(false)
  const [showDeleteDialog, setShowDeleteDialog] = useState(false)

  const setDisabled = useSetDisabled()
  const setModelPolicy = useSetCredentialModelPolicy()
  const setMaxConcurrency = useSetMaxConcurrency()
  const setRateLimitConfig = useSetCredentialRateLimitConfig()
  const setPriority = useSetPriority()
  const resetFailure = useResetFailure()
  const deleteCredential = useDeleteCredential()
  const forceRefresh = useForceRefreshToken()

  const handleToggleDisabled = () => {
    setDisabled.mutate(
      { id: credential.id, disabled: !credential.disabled },
      {
        onSuccess: (res) => {
          toast.success(res.message)
        },
        onError: (err) => {
          toast.error('操作失败: ' + (err as Error).message)
        },
      }
    )
  }

  const handlePriorityChange = () => {
    const newPriority = parseInt(priorityValue, 10)
    if (isNaN(newPriority) || newPriority < 0) {
      toast.error('优先级必须是非负整数')
      return
    }
    setPriority.mutate(
      { id: credential.id, priority: newPriority },
      {
        onSuccess: (res) => {
          toast.success(res.message)
          setEditingPriority(false)
        },
        onError: (err) => {
          toast.error('操作失败: ' + (err as Error).message)
        },
      }
    )
  }

  const handleMaxConcurrencyChange = () => {
    const trimmed = maxConcurrencyValue.trim()
    const parsed = trimmed ? Number.parseInt(trimmed, 10) : undefined
    if (
      trimmed &&
      (parsed === undefined || !Number.isInteger(parsed) || parsed <= 0)
    ) {
      toast.error('并发上限必须是大于 0 的整数，留空表示不限制')
      return
    }

    setMaxConcurrency.mutate(
      { id: credential.id, maxConcurrency: parsed ?? null },
      {
        onSuccess: (res) => {
          toast.success(res.message)
          setEditingMaxConcurrency(false)
        },
        onError: (err) => {
          toast.error('操作失败: ' + (err as Error).message)
        },
      }
    )
  }

  const handleRateLimitConfigChange = () => {
    const trimmedBucketCapacity = bucketCapacityValue.trim()
    const trimmedRefillPerSecond = refillPerSecondValue.trim()
    const parsedBucketCapacity = trimmedBucketCapacity
      ? Number.parseFloat(trimmedBucketCapacity)
      : undefined
    const parsedRefillPerSecond = trimmedRefillPerSecond
      ? Number.parseFloat(trimmedRefillPerSecond)
      : undefined

    if (
      parsedBucketCapacity !== undefined &&
      (!Number.isFinite(parsedBucketCapacity) || parsedBucketCapacity < 0)
    ) {
      toast.error('Bucket 容量必须是大于等于 0 的数字，留空表示跟随全局')
      return
    }
    if (
      parsedRefillPerSecond !== undefined &&
      (!Number.isFinite(parsedRefillPerSecond) || parsedRefillPerSecond < 0)
    ) {
      toast.error('回填速率必须是大于等于 0 的数字，留空表示跟随全局')
      return
    }

    setRateLimitConfig.mutate(
      {
        id: credential.id,
        rateLimitBucketCapacity: parsedBucketCapacity ?? null,
        rateLimitRefillPerSecond: parsedRefillPerSecond ?? null,
      },
      {
        onSuccess: (res) => {
          toast.success(res.message)
          setEditingRateLimitConfig(false)
        },
        onError: (err) => {
          toast.error('操作失败: ' + (err as Error).message)
        },
      }
    )
  }

  const handleReset = () => {
    resetFailure.mutate(credential.id, {
      onSuccess: (res) => {
        toast.success(res.message)
      },
      onError: (err) => {
        toast.error('操作失败: ' + (err as Error).message)
      },
    })
  }

  const handleForceRefresh = () => {
    forceRefresh.mutate(credential.id, {
      onSuccess: (res) => {
        toast.success(res.message)
      },
      onError: (err) => {
        toast.error('刷新失败: ' + (err as Error).message)
      },
    })
  }

  const handleDelete = () => {
    if (!credential.disabled) {
      toast.error('请先禁用凭据再删除')
      setShowDeleteDialog(false)
      return
    }

    deleteCredential.mutate(credential.id, {
      onSuccess: (res) => {
        toast.success(res.message)
        setShowDeleteDialog(false)
      },
      onError: (err) => {
        toast.error('删除失败: ' + (err as Error).message)
      },
    })
  }

  const openModelPolicyDialog = () => {
    setAccountTypeValue(credential.accountType ?? '')
    setAllowedModelsValue(credential.allowedModels ?? [])
    setBlockedModelsValue(credential.blockedModels ?? [])
    setClearRuntimeModelRestrictions(false)
    setShowModelPolicyDialog(true)
  }

  const handleModelPolicySave = () => {
    setModelPolicy.mutate(
      {
        id: credential.id,
        payload: {
          accountType: accountTypeValue.trim() ? accountTypeValue.trim() : null,
          allowedModels: allowedModelsValue.length ? allowedModelsValue : null,
          blockedModels: blockedModelsValue.length ? blockedModelsValue : null,
          clearRuntimeModelRestrictions,
        },
      },
      {
        onSuccess: (res) => {
          toast.success(res.message)
          setShowModelPolicyDialog(false)
        },
        onError: (err) => {
          toast.error('操作失败: ' + (err as Error).message)
        },
      }
    )
  }

  const allowedModelsSummary = summarizeSelectedModels(allowedModelsValue, modelCatalog)
  const blockedModelsSummary = summarizeSelectedModels(blockedModelsValue, modelCatalog)
  const rateLimitOverrideSummary = [
    credential.rateLimitBucketCapacityOverride === undefined ||
    credential.rateLimitBucketCapacityOverride === null
      ? 'Bucket 跟随全局'
      : credential.rateLimitBucketCapacityOverride === 0
        ? 'Bucket 已禁用'
        : `Bucket=${credential.rateLimitBucketCapacityOverride}`,
    credential.rateLimitRefillPerSecondOverride === undefined ||
    credential.rateLimitRefillPerSecondOverride === null
      ? '回填跟随全局'
      : credential.rateLimitRefillPerSecondOverride === 0
        ? '回填已禁用'
        : `回填=${credential.rateLimitRefillPerSecondOverride} token/s`,
  ].join(' / ')
  const policySummary = `允许 ${credential.allowedModels?.length ?? 0} 项 / 禁用 ${credential.blockedModels?.length ?? 0} 项`
  const hasRuntimeRestrictions = (credential.runtimeModelRestrictions?.length ?? 0) > 0
  const balanceSummary = loadingBalance
    ? null
    : balance
      ? `${balance.remaining.toFixed(2)} / ${balance.usageLimit.toFixed(2)}`
      : '未知'
  const balancePercentRemaining = balance ? `${(100 - balance.usagePercentage).toFixed(1)}% 剩余` : null
  const subscriptionLabel = credential.subscriptionTitle || balance?.subscriptionTitle || '未知'
  const bucketSummary =
    credential.rateLimitBucketCapacity !== undefined && credential.rateLimitBucketCapacity !== null
      ? `${(credential.rateLimitBucketTokens ?? 0).toFixed(2)} / ${credential.rateLimitBucketCapacity.toFixed(2)}`
      : null
  const refillSummary =
    credential.rateLimitRefillPerSecond !== undefined && credential.rateLimitRefillPerSecond !== null
      ? `${credential.rateLimitRefillPerSecond.toFixed(2)}${
          credential.rateLimitRefillBasePerSecond !== undefined &&
          credential.rateLimitRefillBasePerSecond !== null
            ? ` / ${credential.rateLimitRefillBasePerSecond.toFixed(2)}`
            : ''
        } token/s`
      : null
  const cooldownSummary =
    credential.cooldownRemainingMs && credential.cooldownRemainingMs > 0
      ? `${(credential.cooldownRemainingMs / 1000).toFixed(1)}s`
      : null
  const nextReadySummary =
    credential.nextReadyInMs !== undefined &&
    credential.nextReadyInMs !== null &&
    credential.nextReadyInMs > 0
      ? `${(credential.nextReadyInMs / 1000).toFixed(1)}s`
      : null

  return (
    <>
      <Card className={credential.isCurrent ? 'ring-2 ring-primary' : ''}>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Checkbox
                checked={selected}
                onCheckedChange={onToggleSelect}
              />
              <CardTitle className="text-lg flex items-center gap-2">
                {credential.email || `凭据 #${credential.id}`}
                {credential.isCurrent && (
                  <Badge variant="success">当前</Badge>
                )}
                {credential.disabled && (
                  <Badge variant="destructive">已禁用</Badge>
                )}
                {credential.disabled && credential.disabledReason && (
                  <Badge variant="outline">{credential.disabledReason}</Badge>
                )}
              </CardTitle>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">启用</span>
              <Switch
                checked={!credential.disabled}
                onCheckedChange={handleToggleDisabled}
                disabled={setDisabled.isPending}
              />
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* 信息摘要 */}
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <InfoTile label="优先级">
              {editingPriority ? (
                <div className="flex flex-wrap items-center gap-1">
                  <Input
                    type="number"
                    value={priorityValue}
                    onChange={(e) => setPriorityValue(e.target.value)}
                    className="h-8 w-20 text-sm"
                    min="0"
                  />
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-8 w-8 p-0"
                    onClick={handlePriorityChange}
                    disabled={setPriority.isPending}
                  >
                    ✓
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-8 w-8 p-0"
                    onClick={() => {
                      setEditingPriority(false)
                      setPriorityValue(String(credential.priority))
                    }}
                  >
                    ✕
                  </Button>
                </div>
              ) : (
                <span
                  className="cursor-pointer hover:underline"
                  onClick={() => setEditingPriority(true)}
                >
                  {credential.priority}
                  <span className="ml-1 text-xs text-muted-foreground">(点击编辑)</span>
                </span>
              )}
            </InfoTile>

            <InfoTile label="并发上限">
              {editingMaxConcurrency ? (
                <div className="flex flex-wrap items-center gap-1">
                  <Input
                    type="number"
                    value={maxConcurrencyValue}
                    onChange={(e) => setMaxConcurrencyValue(e.target.value)}
                    className="h-8 w-24 text-sm"
                    min="1"
                    placeholder="不限"
                  />
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-8 w-8 p-0"
                    onClick={handleMaxConcurrencyChange}
                    disabled={setMaxConcurrency.isPending}
                  >
                    ✓
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-8 w-8 p-0"
                    onClick={() => {
                      setEditingMaxConcurrency(false)
                      setMaxConcurrencyValue(
                        credential.maxConcurrency ? String(credential.maxConcurrency) : ''
                      )
                    }}
                  >
                    ✕
                  </Button>
                </div>
              ) : (
                <span
                  className="cursor-pointer hover:underline"
                  onClick={() => setEditingMaxConcurrency(true)}
                >
                  {credential.maxConcurrency ?? '不限'}
                  <span className="ml-1 text-xs text-muted-foreground">(点击编辑)</span>
                </span>
              )}
            </InfoTile>

            <InfoTile label="订阅与用量">
              <div className="space-y-1">
                <div>{loadingBalance && !credential.subscriptionTitle ? <Loader2 className="h-4 w-4 animate-spin" /> : subscriptionLabel}</div>
                <div className="text-xs font-normal text-muted-foreground">
                  剩余：
                  {loadingBalance ? (
                    <span className="ml-1 inline-flex items-center gap-1">
                      <Loader2 className="h-3 w-3 animate-spin" />
                      加载中
                    </span>
                  ) : (
                    <>
                      <span className="ml-1">{balanceSummary}</span>
                      {balancePercentRemaining && <span className="ml-1">({balancePercentRemaining})</span>}
                    </>
                  )}
                </div>
              </div>
            </InfoTile>

            <InfoTile label="最后调用">
              {formatLastUsed(credential.lastUsedAt)}
            </InfoTile>
          </div>

          <div className="rounded-lg border border-dashed px-3 py-2.5">
            <div className="flex flex-wrap items-start justify-between gap-2">
              <div className="min-w-0 flex-1">
                <div className="text-xs text-muted-foreground">凭据级限速覆盖</div>
                {!editingRateLimitConfig && (
                  <div className="mt-1 text-sm font-medium leading-snug">
                    {rateLimitOverrideSummary}
                  </div>
                )}
              </div>
              {!editingRateLimitConfig && (
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-7 px-2 text-xs"
                  onClick={() => setEditingRateLimitConfig(true)}
                >
                  编辑
                </Button>
              )}
            </div>
            {editingRateLimitConfig && (
              <div className="mt-3 flex flex-wrap items-center gap-2">
                <Input
                  type="number"
                  value={bucketCapacityValue}
                  onChange={(e) => setBucketCapacityValue(e.target.value)}
                  className="h-8 w-28 text-sm"
                  min="0"
                  step="0.1"
                  placeholder="容量"
                />
                <Input
                  type="number"
                  value={refillPerSecondValue}
                  onChange={(e) => setRefillPerSecondValue(e.target.value)}
                  className="h-8 w-32 text-sm"
                  min="0"
                  step="0.1"
                  placeholder="回填 token/s"
                />
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-8 w-8 p-0"
                  onClick={handleRateLimitConfigChange}
                  disabled={setRateLimitConfig.isPending}
                >
                  ✓
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-8 w-8 p-0"
                  onClick={() => {
                    setEditingRateLimitConfig(false)
                    setBucketCapacityValue(
                      credential.rateLimitBucketCapacityOverride !== undefined &&
                      credential.rateLimitBucketCapacityOverride !== null
                        ? String(credential.rateLimitBucketCapacityOverride)
                        : ''
                    )
                    setRefillPerSecondValue(
                      credential.rateLimitRefillPerSecondOverride !== undefined &&
                      credential.rateLimitRefillPerSecondOverride !== null
                        ? String(credential.rateLimitRefillPerSecondOverride)
                        : ''
                    )
                  }}
                >
                  ✕
                </Button>
                <span className="text-xs text-muted-foreground">
                  留空跟随全局，0 为仅禁用该账号
                </span>
              </div>
            )}
          </div>

          <div className="flex flex-wrap gap-2">
            <Badge variant={credential.failureCount > 0 ? 'destructive' : 'outline'}>
              失败 {credential.failureCount}
            </Badge>
            <Badge variant={credential.refreshFailureCount > 0 ? 'destructive' : 'outline'}>
              刷新失败 {credential.refreshFailureCount}
            </Badge>
            <Badge variant="outline">成功 {credential.successCount}</Badge>
            <Badge variant="outline">
              并发 {credential.inFlight}{credential.maxConcurrency ? ` / ${credential.maxConcurrency}` : ''}
            </Badge>
            <Badge variant="secondary" className="max-w-full break-all">
              账号类型 {credential.accountType || '未设置'}
            </Badge>
            <Badge variant="secondary">{policySummary}</Badge>
            {bucketSummary && <Badge variant="outline">Bucket {bucketSummary}</Badge>}
            {refillSummary && <Badge variant="outline">回填 {refillSummary}</Badge>}
            {cooldownSummary && <Badge variant="warning">429 冷却 {cooldownSummary}</Badge>}
            {credential.rateLimitHitStreak > 0 && (
              <Badge variant="warning">连续 429 {credential.rateLimitHitStreak}</Badge>
            )}
            {nextReadySummary && <Badge variant="outline">下次可调度 {nextReadySummary}</Badge>}
            {credential.hasProxy && <Badge variant="outline">已配置代理</Badge>}
            {credential.hasProfileArn && <Badge variant="secondary">有 Profile ARN</Badge>}
          </div>

          {hasRuntimeRestrictions && (
            <div className="space-y-2 rounded-lg border border-dashed border-amber-300 bg-amber-50/40 px-3 py-2.5">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="text-xs text-muted-foreground">运行时临时限制</div>
                <Badge variant="warning">{credential.runtimeModelRestrictions?.length ?? 0} 条</Badge>
              </div>
              <div className="flex flex-wrap gap-2">
                {credential.runtimeModelRestrictions?.map((restriction) => (
                  <Badge key={`${restriction.model}-${restriction.expiresAt}`} variant="outline">
                    {restriction.model} 至 {formatRestrictionExpiresAt(restriction.expiresAt)}
                  </Badge>
                ))}
              </div>
            </div>
          )}

          {credential.hasProxy && credential.proxyUrl && (
            <div className="text-xs text-muted-foreground">
              代理地址：
              <span className="ml-1 break-all text-foreground">{credential.proxyUrl}</span>
            </div>
          )}

          {/* 操作按钮 */}
          <div className="flex flex-wrap gap-2 pt-2 border-t">
            <Button
              size="sm"
              variant="outline"
              onClick={handleReset}
              disabled={resetFailure.isPending || (credential.failureCount === 0 && credential.refreshFailureCount === 0)}
            >
              <RefreshCw className="h-4 w-4 mr-1" />
              重置失败
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={handleForceRefresh}
              disabled={forceRefresh.isPending || credential.disabled}
              title={credential.disabled ? '已禁用的凭据无法刷新 Token' : '强制刷新 Token'}
            >
              <RefreshCw className={`h-4 w-4 mr-1 ${forceRefresh.isPending ? 'animate-spin' : ''}`} />
              刷新 Token
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={() => {
                const newPriority = Math.max(0, credential.priority - 1)
                setPriority.mutate(
                  { id: credential.id, priority: newPriority },
                  {
                    onSuccess: (res) => toast.success(res.message),
                    onError: (err) => toast.error('操作失败: ' + (err as Error).message),
                  }
                )
              }}
              disabled={setPriority.isPending || credential.priority === 0}
            >
              <ChevronUp className="h-4 w-4 mr-1" />
              提高优先级
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={() => {
                const newPriority = credential.priority + 1
                setPriority.mutate(
                  { id: credential.id, priority: newPriority },
                  {
                    onSuccess: (res) => toast.success(res.message),
                    onError: (err) => toast.error('操作失败: ' + (err as Error).message),
                  }
                )
              }}
              disabled={setPriority.isPending}
            >
              <ChevronDown className="h-4 w-4 mr-1" />
              降低优先级
            </Button>
            <Button
              size="sm"
              variant="default"
              onClick={() => onViewBalance(credential.id)}
            >
              <Wallet className="h-4 w-4 mr-1" />
              查看余额
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={openModelPolicyDialog}
              disabled={setModelPolicy.isPending}
            >
              模型策略
            </Button>
            <Button
              size="sm"
              variant="destructive"
              onClick={() => setShowDeleteDialog(true)}
              disabled={!credential.disabled}
              title={!credential.disabled ? '需要先禁用凭据才能删除' : undefined}
            >
              <Trash2 className="h-4 w-4 mr-1" />
              删除
            </Button>
          </div>
        </CardContent>
      </Card>

      <Dialog open={showModelPolicyDialog} onOpenChange={setShowModelPolicyDialog}>
        <DialogContent className="sm:max-w-3xl max-h-[85vh] flex flex-col">
          <DialogHeader>
            <DialogTitle>编辑模型策略</DialogTitle>
            <DialogDescription>
              账号类型策略先命中，再叠加此账号自己的允许/禁用列表。运行时临时限制可在这里一并清空。
            </DialogDescription>
          </DialogHeader>
          <div className="flex-1 space-y-4 overflow-y-auto pr-1">
            <AccountTypeInput
              id={`account-type-${credential.id}`}
              label="账号类型"
              value={accountTypeValue}
              onChange={setAccountTypeValue}
              suggestions={accountTypeSuggestions}
              placeholder="优先选择已有类型，也可直接新建"
            />

            <div className="grid gap-4 lg:grid-cols-2">
              <details className="rounded-lg border border-input bg-muted/10 p-3">
                <summary className="cursor-pointer">
                  <div className="flex items-start justify-between gap-3">
                    <div className="space-y-1">
                      <div className="text-sm font-medium">账号级额外允许模型</div>
                      <p className="text-xs text-muted-foreground">{allowedModelsSummary}</p>
                    </div>
                    <Badge variant="outline">{allowedModelsValue.length} 已选</Badge>
                  </div>
                </summary>
                <div className="mt-3">
                  <ModelSelector
                    label="账号级额外允许模型"
                    selectedValues={allowedModelsValue}
                    onChange={setAllowedModelsValue}
                    options={modelCatalog}
                    hideHeader
                  />
                </div>
              </details>

              <details className="rounded-lg border border-input bg-muted/10 p-3">
                <summary className="cursor-pointer">
                  <div className="flex items-start justify-between gap-3">
                    <div className="space-y-1">
                      <div className="text-sm font-medium">账号级额外禁用模型</div>
                      <p className="text-xs text-muted-foreground">{blockedModelsSummary}</p>
                    </div>
                    <Badge variant="outline">{blockedModelsValue.length} 已选</Badge>
                  </div>
                </summary>
                <div className="mt-3">
                  <ModelSelector
                    label="账号级额外禁用模型"
                    selectedValues={blockedModelsValue}
                    onChange={setBlockedModelsValue}
                    options={modelCatalog}
                    hideHeader
                  />
                </div>
              </details>
            </div>

            <div className="flex items-center gap-2 rounded-lg border border-dashed p-3">
              <Checkbox
                checked={clearRuntimeModelRestrictions}
                onCheckedChange={(checked) => setClearRuntimeModelRestrictions(Boolean(checked))}
              />
              <div className="space-y-1">
                <div className="text-sm font-medium">保存时清空运行时临时限制</div>
                <p className="text-xs text-muted-foreground">
                  当前 {credential.runtimeModelRestrictions?.length ?? 0} 条。适合上游刚开权限后手动重试。
                </p>
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setShowModelPolicyDialog(false)}
              disabled={setModelPolicy.isPending}
            >
              取消
            </Button>
            <Button onClick={handleModelPolicySave} disabled={setModelPolicy.isPending}>
              保存
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* 删除确认对话框 */}
      <Dialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>确认删除凭据</DialogTitle>
            <DialogDescription>
              您确定要删除凭据 #{credential.id} 吗？此操作无法撤销。
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setShowDeleteDialog(false)}
              disabled={deleteCredential.isPending}
            >
              取消
            </Button>
            <Button
              variant="destructive"
              onClick={handleDelete}
              disabled={deleteCredential.isPending || !credential.disabled}
            >
              确认删除
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}
