import { useState, useEffect } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { toast } from 'sonner'
import { useLoadBalancingMode, useSetLoadBalancingMode } from '@/hooks/use-credentials'
import { extractErrorMessage } from '@/lib/utils'
import { Save, AlertCircle } from 'lucide-react'

export function SettingsPage() {
  const { data: loadBalancingData, isLoading: isLoadingMode } = useLoadBalancingMode()
  const { mutate: setLoadBalancingMode, isPending: isSettingMode } = useSetLoadBalancingMode()

  const [queueMaxSizeInput, setQueueMaxSizeInput] = useState('0')
  const [queueMaxWaitMsInput, setQueueMaxWaitMsInput] = useState('0')
  const [rateLimitCooldownMsInput, setRateLimitCooldownMsInput] = useState('2000')
  const [defaultMaxConcurrencyInput, setDefaultMaxConcurrencyInput] = useState('')
  const [rateLimitBucketCapacityInput, setRateLimitBucketCapacityInput] = useState('3')
  const [rateLimitRefillPerSecondInput, setRateLimitRefillPerSecondInput] = useState('1')
  const [rateLimitRefillMinPerSecondInput, setRateLimitRefillMinPerSecondInput] = useState('0.2')
  const [rateLimitRefillRecoveryStepInput, setRateLimitRefillRecoveryStepInput] = useState('0.1')
  const [rateLimitRefillBackoffFactorInput, setRateLimitRefillBackoffFactorInput] = useState('0.5')

  useEffect(() => {
    if (!loadBalancingData) {
      return
    }
    setQueueMaxSizeInput(String(loadBalancingData.queueMaxSize))
    setQueueMaxWaitMsInput(String(loadBalancingData.queueMaxWaitMs))
    setRateLimitCooldownMsInput(String(loadBalancingData.rateLimitCooldownMs))
    setDefaultMaxConcurrencyInput(loadBalancingData.defaultMaxConcurrency ? String(loadBalancingData.defaultMaxConcurrency) : '')
    setRateLimitBucketCapacityInput(String(loadBalancingData.rateLimitBucketCapacity))
    setRateLimitRefillPerSecondInput(String(loadBalancingData.rateLimitRefillPerSecond))
    setRateLimitRefillMinPerSecondInput(String(loadBalancingData.rateLimitRefillMinPerSecond))
    setRateLimitRefillRecoveryStepInput(String(loadBalancingData.rateLimitRefillRecoveryStepPerSuccess))
    setRateLimitRefillBackoffFactorInput(String(loadBalancingData.rateLimitRefillBackoffFactor))
  }, [
    loadBalancingData?.queueMaxSize,
    loadBalancingData?.queueMaxWaitMs,
    loadBalancingData?.rateLimitCooldownMs,
    loadBalancingData?.defaultMaxConcurrency,
    loadBalancingData?.rateLimitBucketCapacity,
    loadBalancingData?.rateLimitRefillPerSecond,
    loadBalancingData?.rateLimitRefillMinPerSecond,
    loadBalancingData?.rateLimitRefillRecoveryStepPerSuccess,
    loadBalancingData?.rateLimitRefillBackoffFactor,
  ])

  const handleSaveQueueSettings = () => {
    const parsedQueueMaxSize = queueMaxSizeInput.trim() === '' ? 0 : parseInt(queueMaxSizeInput, 10)
    const parsedQueueMaxWaitMs = queueMaxWaitMsInput.trim() === '' ? 0 : parseInt(queueMaxWaitMsInput, 10)
    const parsedRateLimitCooldownMs = rateLimitCooldownMsInput.trim() === '' ? 0 : parseInt(rateLimitCooldownMsInput, 10)
    const parsedDefaultMaxConcurrency = defaultMaxConcurrencyInput.trim() === '' ? 0 : parseInt(defaultMaxConcurrencyInput, 10)
    const parsedRateLimitBucketCapacity = rateLimitBucketCapacityInput.trim() === '' ? 0 : Number.parseFloat(rateLimitBucketCapacityInput)
    const parsedRateLimitRefillPerSecond = rateLimitRefillPerSecondInput.trim() === '' ? 0 : Number.parseFloat(rateLimitRefillPerSecondInput)
    const parsedRateLimitRefillMinPerSecond = rateLimitRefillMinPerSecondInput.trim() === '' ? 0 : Number.parseFloat(rateLimitRefillMinPerSecondInput)
    const parsedRateLimitRefillRecoveryStep = rateLimitRefillRecoveryStepInput.trim() === '' ? 0 : Number.parseFloat(rateLimitRefillRecoveryStepInput)
    const parsedRateLimitRefillBackoffFactor = rateLimitRefillBackoffFactorInput.trim() === '' ? 0 : Number.parseFloat(rateLimitRefillBackoffFactorInput)

    if (
      Number.isNaN(parsedQueueMaxSize) ||
      Number.isNaN(parsedQueueMaxWaitMs) ||
      Number.isNaN(parsedRateLimitCooldownMs) ||
      Number.isNaN(parsedDefaultMaxConcurrency) ||
      Number.isNaN(parsedRateLimitBucketCapacity) ||
      Number.isNaN(parsedRateLimitRefillPerSecond) ||
      Number.isNaN(parsedRateLimitRefillMinPerSecond) ||
      Number.isNaN(parsedRateLimitRefillRecoveryStep) ||
      Number.isNaN(parsedRateLimitRefillBackoffFactor) ||
      parsedQueueMaxSize < 0 ||
      parsedQueueMaxWaitMs < 0 ||
      parsedRateLimitCooldownMs < 0 ||
      parsedDefaultMaxConcurrency < 0 ||
      parsedRateLimitBucketCapacity < 0 ||
      parsedRateLimitRefillPerSecond < 0 ||
      parsedRateLimitRefillMinPerSecond < 0 ||
      parsedRateLimitRefillRecoveryStep < 0
    ) {
      toast.error('调度参数必须是大于等于 0 的数字')
      return
    }

    if (
      parsedRateLimitRefillBackoffFactor < 0.05 ||
      parsedRateLimitRefillBackoffFactor > 1
    ) {
      toast.error('429 衰减系数必须在 0.05 到 1 之间')
      return
    }

    if (
      parsedRateLimitRefillPerSecond > 0 &&
      parsedRateLimitRefillMinPerSecond > parsedRateLimitRefillPerSecond
    ) {
      toast.error('最小回填速率不能大于基础回填速率')
      return
    }

    setLoadBalancingMode(
      {
        queueMaxSize: parsedQueueMaxSize,
        queueMaxWaitMs: parsedQueueMaxWaitMs,
        rateLimitCooldownMs: parsedRateLimitCooldownMs,
        defaultMaxConcurrency: parsedDefaultMaxConcurrency,
        rateLimitBucketCapacity: parsedRateLimitBucketCapacity,
        rateLimitRefillPerSecond: parsedRateLimitRefillPerSecond,
        rateLimitRefillMinPerSecond: parsedRateLimitRefillMinPerSecond,
        rateLimitRefillRecoveryStepPerSuccess: parsedRateLimitRefillRecoveryStep,
        rateLimitRefillBackoffFactor: parsedRateLimitRefillBackoffFactor,
      },
      {
        onSuccess: () => {
          toast.success('调度配置已更新')
        },
        onError: (error) => {
          toast.error(`保存失败: ${extractErrorMessage(error)}`)
        }
      }
    )
  }

  if (isLoadingMode) {
    return (
      <div className="flex h-[200px] items-center justify-center">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary mx-auto"></div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-2">
        <h2 className="text-2xl font-semibold tracking-tight">调度与并发配置</h2>
        <p className="text-muted-foreground">管理此节点的全局令牌桶限制以及服务并发上限设置</p>
      </div>

      <Card className="border-muted shadow-sm">
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>全局调度参数</CardTitle>
              <CardDescription>当以负载均衡或优先级模式分发请求时生效的核心引擎参数</CardDescription>
            </div>
            {loadBalancingData && (
              <div className="flex flex-col items-end">
                <div className="flex items-center gap-2">
                  <Badge variant="secondary" className="px-3 py-1">
                    当前模式: {loadBalancingData.mode === 'balanced' ? '均衡负载' : '优先级模式'}
                  </Badge>
                  {loadBalancingData.waitingRequests > 0 && (
                    <Badge variant="destructive" className="animate-pulse">
                      {loadBalancingData.waitingRequests} 排队中
                    </Badge>
                  )}
                </div>
              </div>
            )}
          </div>
        </CardHeader>

        <CardContent className="grid gap-x-8 gap-y-6 md:grid-cols-2">
          
          <div className="space-y-4">
            <h3 className="text-sm font-semibold flex items-center text-primary">队列控制配置</h3>
            <div className="grid gap-4 bg-muted/30 p-4 rounded-lg">
              <div className="space-y-2">
                <label className="text-sm font-medium" htmlFor="queueMaxSize">
                  最大排队数量 (请求数)
                </label>
                <Input
                  id="queueMaxSize"
                  type="number"
                  min="0"
                  step="1"
                  value={queueMaxSizeInput}
                  onChange={(e) => setQueueMaxSizeInput(e.target.value)}
                  placeholder="0 表示关闭等待队列"
                />
              </div>

              <div className="space-y-2">
                <label className="text-sm font-medium" htmlFor="queueMaxWaitMs">
                  最大等待时间 (毫秒)
                </label>
                <Input
                  id="queueMaxWaitMs"
                  type="number"
                  min="0"
                  step="100"
                  value={queueMaxWaitMsInput}
                  onChange={(e) => setQueueMaxWaitMsInput(e.target.value)}
                  placeholder="0 表示关闭等待队列"
                />
              </div>
            </div>
          </div>

          <div className="space-y-4">
            <h3 className="text-sm font-semibold flex items-center text-primary">网络及接口限流</h3>
            <div className="grid gap-4 bg-muted/30 p-4 rounded-lg">
              <div className="space-y-2">
                <label className="text-sm font-medium" htmlFor="rateLimitCooldownMs">
                  429 异常冷却时间 (毫秒)
                </label>
                <Input
                  id="rateLimitCooldownMs"
                  type="number"
                  min="0"
                  step="100"
                  value={rateLimitCooldownMsInput}
                  onChange={(e) => setRateLimitCooldownMsInput(e.target.value)}
                  placeholder="0 表示关闭 429 冷却"
                />
              </div>

              <div className="space-y-2">
                <label className="text-sm font-medium" htmlFor="defaultMaxConcurrency">
                  默认账号并发上限
                </label>
                <Input
                  id="defaultMaxConcurrency"
                  type="number"
                  min="0"
                  step="1"
                  value={defaultMaxConcurrencyInput}
                  onChange={(e) => setDefaultMaxConcurrencyInput(e.target.value)}
                  placeholder="留空或 0 表示全局不限制"
                />
              </div>
            </div>
          </div>

          <div className="space-y-4 md:col-span-2">
            <h3 className="text-sm font-semibold flex items-center text-primary">弹性令牌桶流量控制</h3>
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3 bg-muted/30 p-4 rounded-lg">
              <div className="space-y-2">
                <label className="text-sm font-medium" htmlFor="rateLimitBucketCapacity">
                  Bucket 容量上限
                </label>
                <Input
                  id="rateLimitBucketCapacity"
                  type="number"
                  min="0"
                  step="0.1"
                  value={rateLimitBucketCapacityInput}
                  onChange={(e) => setRateLimitBucketCapacityInput(e.target.value)}
                  placeholder="0 表示关闭 Token Bucket"
                />
              </div>

              <div className="space-y-2">
                <label className="text-sm font-medium" htmlFor="rateLimitRefillPerSecond">
                  基础回填速率 (Token/s)
                </label>
                <Input
                  id="rateLimitRefillPerSecond"
                  type="number"
                  min="0"
                  step="0.1"
                  value={rateLimitRefillPerSecondInput}
                  onChange={(e) => setRateLimitRefillPerSecondInput(e.target.value)}
                  placeholder="0 表示关闭 Token Bucket"
                />
              </div>

              <div className="space-y-2">
                <label className="text-sm font-medium" htmlFor="rateLimitRefillMinPerSecond">
                  最小回填速率 (Token/s)
                </label>
                <Input
                  id="rateLimitRefillMinPerSecond"
                  type="number"
                  min="0"
                  step="0.1"
                  value={rateLimitRefillMinPerSecondInput}
                  onChange={(e) => setRateLimitRefillMinPerSecondInput(e.target.value)}
                />
              </div>

              <div className="space-y-2">
                <label className="text-sm font-medium" htmlFor="rateLimitRefillRecoveryStep">
                  接口成功时速度探测回升步长
                </label>
                <Input
                  id="rateLimitRefillRecoveryStep"
                  type="number"
                  min="0"
                  step="0.1"
                  value={rateLimitRefillRecoveryStepInput}
                  onChange={(e) => setRateLimitRefillRecoveryStepInput(e.target.value)}
                />
              </div>

              <div className="space-y-2">
                <label className="text-sm font-medium" htmlFor="rateLimitRefillBackoffFactor">
                  因429退避导致的速率衰退比系数
                </label>
                <Input
                  id="rateLimitRefillBackoffFactor"
                  type="number"
                  min="0.05"
                  max="1"
                  step="0.05"
                  value={rateLimitRefillBackoffFactorInput}
                  onChange={(e) => setRateLimitRefillBackoffFactorInput(e.target.value)}
                />
              </div>
            </div>
          </div>
          
        </CardContent>

        <div className="px-6 py-4 bg-muted/20 border-t flex items-center justify-between flex-wrap gap-4 rounded-b-lg">
          <div className="flex text-sm text-muted-foreground max-w-2xl gap-2">
            <AlertCircle className="h-5 w-5 shrink-0 text-yellow-600 dark:text-yellow-500" />
            <p>
              `defaultMaxConcurrency` 是未单独设置账号维度的默认回退并发上限。推荐初始化组合: <code className="bg-muted px-1 rounded">queueMaxWaitMs=2000</code>、<code className="bg-muted px-1 rounded">queueMaxSize=峰值2倍</code>、<code className="bg-muted px-1 rounded">rateLimitCooldownMs=3000</code>。
            </p>
          </div>
          <Button
            onClick={handleSaveQueueSettings}
            disabled={isSettingMode}
            className="shrink-0"
          >
            <Save className="h-4 w-4 mr-2" />
            保存所有配置
          </Button>
        </div>
      </Card>
    </div>
  )
}
