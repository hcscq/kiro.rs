import { useState } from 'react'
import { toast } from 'sonner'
import { RefreshCw, ChevronUp, ChevronDown, Wallet, Trash2, Loader2 } from 'lucide-react'
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
import type { CredentialStatusItem, BalanceResponse } from '@/types/api'
import {
  useSetDisabled,
  useSetCredentialRateLimitConfig,
  useSetMaxConcurrency,
  useSetPriority,
  useResetFailure,
  useDeleteCredential,
  useForceRefreshToken,
} from '@/hooks/use-credentials'

interface CredentialCardProps {
  credential: CredentialStatusItem
  onViewBalance: (id: number) => void
  selected: boolean
  onToggleSelect: () => void
  balance: BalanceResponse | null
  loadingBalance: boolean
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

export function CredentialCard({
  credential,
  onViewBalance,
  selected,
  onToggleSelect,
  balance,
  loadingBalance,
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
  const [showDeleteDialog, setShowDeleteDialog] = useState(false)

  const setDisabled = useSetDisabled()
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
          {/* 信息网格 */}
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <span className="text-muted-foreground">优先级：</span>
              {editingPriority ? (
                <div className="inline-flex items-center gap-1 ml-1">
                  <Input
                    type="number"
                    value={priorityValue}
                    onChange={(e) => setPriorityValue(e.target.value)}
                    className="w-16 h-7 text-sm"
                    min="0"
                  />
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-7 w-7 p-0"
                    onClick={handlePriorityChange}
                    disabled={setPriority.isPending}
                  >
                    ✓
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-7 w-7 p-0"
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
                  className="font-medium cursor-pointer hover:underline ml-1"
                  onClick={() => setEditingPriority(true)}
                >
                  {credential.priority}
                  <span className="text-xs text-muted-foreground ml-1">(点击编辑)</span>
                </span>
              )}
            </div>
            <div>
              <span className="text-muted-foreground">并发上限：</span>
              {editingMaxConcurrency ? (
                <div className="inline-flex items-center gap-1 ml-1">
                  <Input
                    type="number"
                    value={maxConcurrencyValue}
                    onChange={(e) => setMaxConcurrencyValue(e.target.value)}
                    className="w-20 h-7 text-sm"
                    min="1"
                    placeholder="不限"
                  />
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-7 w-7 p-0"
                    onClick={handleMaxConcurrencyChange}
                    disabled={setMaxConcurrency.isPending}
                  >
                    ✓
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-7 w-7 p-0"
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
                  className="font-medium cursor-pointer hover:underline ml-1"
                  onClick={() => setEditingMaxConcurrency(true)}
                >
                  {credential.maxConcurrency ?? '不限'}
                  <span className="text-xs text-muted-foreground ml-1">(点击编辑)</span>
                </span>
              )}
            </div>
            <div className="col-span-2">
              <span className="text-muted-foreground">凭据级限速覆盖：</span>
              {editingRateLimitConfig ? (
                <div className="mt-2 flex flex-wrap items-center gap-2">
                  <Input
                    type="number"
                    value={bucketCapacityValue}
                    onChange={(e) => setBucketCapacityValue(e.target.value)}
                    className="w-28 h-7 text-sm"
                    min="0"
                    step="0.1"
                    placeholder="容量"
                  />
                  <Input
                    type="number"
                    value={refillPerSecondValue}
                    onChange={(e) => setRefillPerSecondValue(e.target.value)}
                    className="w-32 h-7 text-sm"
                    min="0"
                    step="0.1"
                    placeholder="回填 token/s"
                  />
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-7 w-7 p-0"
                    onClick={handleRateLimitConfigChange}
                    disabled={setRateLimitConfig.isPending}
                  >
                    ✓
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-7 w-7 p-0"
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
              ) : (
                <span
                  className="font-medium cursor-pointer hover:underline ml-1"
                  onClick={() => setEditingRateLimitConfig(true)}
                >
                  {credential.rateLimitBucketCapacityOverride === undefined ||
                  credential.rateLimitBucketCapacityOverride === null
                    ? 'Bucket 跟随全局'
                    : credential.rateLimitBucketCapacityOverride === 0
                      ? 'Bucket 已禁用'
                      : `Bucket=${credential.rateLimitBucketCapacityOverride}`}
                  {' / '}
                  {credential.rateLimitRefillPerSecondOverride === undefined ||
                  credential.rateLimitRefillPerSecondOverride === null
                    ? '回填跟随全局'
                    : credential.rateLimitRefillPerSecondOverride === 0
                      ? '回填已禁用'
                      : `回填=${credential.rateLimitRefillPerSecondOverride} token/s`}
                  <span className="text-xs text-muted-foreground ml-1">(点击编辑)</span>
                </span>
              )}
            </div>
            <div>
              <span className="text-muted-foreground">失败次数：</span>
              <span className={credential.failureCount > 0 ? 'text-red-500 font-medium' : ''}>
                {credential.failureCount}
              </span>
            </div>
            <div>
              <span className="text-muted-foreground">刷新失败：</span>
              <span className={credential.refreshFailureCount > 0 ? 'text-red-500 font-medium' : ''}>
                {credential.refreshFailureCount}
              </span>
            </div>
            <div>
              <span className="text-muted-foreground">订阅等级：</span>
              <span className="font-medium">
                {credential.subscriptionTitle ? credential.subscriptionTitle : loadingBalance ? (
                  <Loader2 className="inline w-3 h-3 animate-spin" />
                ) : balance?.subscriptionTitle || '未知'}
              </span>
            </div>
            <div>
              <span className="text-muted-foreground">成功次数：</span>
              <span className="font-medium">{credential.successCount}</span>
            </div>
            <div>
              <span className="text-muted-foreground">当前并发：</span>
              <span className="font-medium">
                {credential.inFlight}
                {credential.maxConcurrency ? ` / ${credential.maxConcurrency}` : ''}
              </span>
            </div>
            {credential.rateLimitBucketCapacity !== undefined && credential.rateLimitBucketCapacity !== null && (
              <div>
                <span className="text-muted-foreground">Bucket：</span>
                <span className="font-medium">
                  {(credential.rateLimitBucketTokens ?? 0).toFixed(2)} / {credential.rateLimitBucketCapacity.toFixed(2)}
                </span>
              </div>
            )}
            {credential.cooldownRemainingMs && credential.cooldownRemainingMs > 0 && (
              <div>
                <span className="text-muted-foreground">429 冷却：</span>
                <span className="font-medium text-amber-600">
                  {(credential.cooldownRemainingMs / 1000).toFixed(1)}s
                </span>
              </div>
            )}
            {(credential.rateLimitRefillPerSecond !== undefined && credential.rateLimitRefillPerSecond !== null) && (
              <div>
                <span className="text-muted-foreground">当前回填：</span>
                <span className="font-medium">
                  {credential.rateLimitRefillPerSecond.toFixed(2)}
                  {credential.rateLimitRefillBasePerSecond !== undefined && credential.rateLimitRefillBasePerSecond !== null
                    ? ` / ${credential.rateLimitRefillBasePerSecond.toFixed(2)}`
                    : ''}
                  {' '}token/s
                </span>
              </div>
            )}
            {credential.rateLimitHitStreak > 0 && (
              <div>
                <span className="text-muted-foreground">连续 429：</span>
                <span className="font-medium text-amber-600">{credential.rateLimitHitStreak}</span>
              </div>
            )}
            {credential.nextReadyInMs !== undefined && credential.nextReadyInMs !== null && credential.nextReadyInMs > 0 && (
              <div>
                <span className="text-muted-foreground">下次可调度：</span>
                <span className="font-medium">
                  {(credential.nextReadyInMs / 1000).toFixed(1)}s
                </span>
              </div>
            )}
            <div className="col-span-2">
              <span className="text-muted-foreground">最后调用：</span>
              <span className="font-medium">{formatLastUsed(credential.lastUsedAt)}</span>
            </div>
            <div className="col-span-2">
              <span className="text-muted-foreground">剩余用量：</span>
              {loadingBalance ? (
                <span className="text-sm ml-1">
                  <Loader2 className="inline w-3 h-3 animate-spin" /> 加载中...
                </span>
              ) : balance ? (
                <span className="font-medium ml-1">
                  {balance.remaining.toFixed(2)} / {balance.usageLimit.toFixed(2)}
                  <span className="text-xs text-muted-foreground ml-1">
                    ({(100 - balance.usagePercentage).toFixed(1)}% 剩余)
                  </span>
                </span>
              ) : (
                <span className="text-sm text-muted-foreground ml-1">未知</span>
              )}
            </div>
            {credential.hasProxy && (
              <div className="col-span-2">
                <span className="text-muted-foreground">代理：</span>
                <span className="font-medium">{credential.proxyUrl}</span>
              </div>
            )}
            {credential.hasProfileArn && (
              <div className="col-span-2">
                <Badge variant="secondary">有 Profile ARN</Badge>
              </div>
            )}
          </div>

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
