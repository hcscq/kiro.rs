import { useState, useEffect } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { toast } from 'sonner'
import {
  useLoadBalancingMode,
  useModelCapabilitiesConfig,
  useSetLoadBalancingMode,
  useSetModelCapabilitiesConfig,
} from '@/hooks/use-credentials'
import { extractErrorMessage } from '@/lib/utils'
import { Save, AlertCircle } from 'lucide-react'
import type { ModelSupportPolicy, RequestWeightingConfig } from '@/types/api'

type RequestWeightingNumericField = Exclude<keyof RequestWeightingConfig, 'enabled'>

type RequestWeightingInputState = Record<RequestWeightingNumericField, string>

const DEFAULT_REQUEST_WEIGHTING: RequestWeightingConfig = {
  enabled: true,
  baseWeight: 1,
  maxWeight: 2.5,
  toolsBonus: 0.4,
  largeMaxTokensThreshold: 8000,
  largeMaxTokensBonus: 0.25,
  largeInputTokensThreshold: 12000,
  largeInputTokensBonus: 0.25,
  veryLargeInputTokensThreshold: 24000,
  veryLargeInputTokensBonus: 0.35,
  thinkingBonus: 0.35,
  heavyThinkingBudgetThreshold: 24000,
  heavyThinkingBudgetBonus: 0.35,
}

const REQUEST_WEIGHTING_FIELD_SECTIONS: Array<{
  title: string
  description: string
  fields: Array<{
    key: RequestWeightingNumericField
    label: string
    step: string
    min: string
    placeholder: string
    hint: string
  }>
}> = [
  {
    title: '权重基线',
    description: '决定所有请求的基础消耗以及单次请求可放大的上限。',
    fields: [
      {
        key: 'baseWeight',
        label: '基础权重',
        step: '0.1',
        min: '0.1',
        placeholder: '轻请求通常保持 1.0',
        hint: '每个请求至少消耗多少 bucket 配额。',
      },
      {
        key: 'maxWeight',
        label: '最大权重',
        step: '0.1',
        min: '0.1',
        placeholder: '避免重请求无限放大',
        hint: '最终权重会被 clamp 到这个上限。',
      },
    ],
  },
  {
    title: '大请求判定',
    description: '针对大 `maxTokens` 和大输入量请求，额外增加一次请求的令牌消耗。',
    fields: [
      {
        key: 'largeMaxTokensThreshold',
        label: '大 maxTokens 阈值',
        step: '1',
        min: '0',
        placeholder: '例如 4000',
        hint: '请求声明的 `max_tokens` 超过此值时增加权重。',
      },
      {
        key: 'largeMaxTokensBonus',
        label: '大 maxTokens 加权',
        step: '0.1',
        min: '0',
        placeholder: '例如 0.5',
        hint: '命中大 `max_tokens` 阈值后额外增加的权重。',
      },
      {
        key: 'largeInputTokensThreshold',
        label: '大输入阈值',
        step: '1',
        min: '0',
        placeholder: '例如 8000',
        hint: '估算输入 token 超过此值时增加权重。',
      },
      {
        key: 'largeInputTokensBonus',
        label: '大输入加权',
        step: '0.1',
        min: '0',
        placeholder: '例如 0.5',
        hint: '命中大输入阈值后额外增加的权重。',
      },
      {
        key: 'veryLargeInputTokensThreshold',
        label: '超大输入阈值',
        step: '1',
        min: '0',
        placeholder: '例如 20000',
        hint: '更重的代码/上下文请求超过此值时再次增加权重。',
      },
      {
        key: 'veryLargeInputTokensBonus',
        label: '超大输入加权',
        step: '0.1',
        min: '0',
        placeholder: '例如 0.5',
        hint: '命中超大输入阈值后额外增加的权重。',
      },
    ],
  },
  {
    title: '工具与 Thinking',
    description: '针对带工具、开启 thinking 或 thinking budget 很高的请求做附加权重。',
    fields: [
      {
        key: 'toolsBonus',
        label: 'Tools 加权',
        step: '0.1',
        min: '0',
        placeholder: '例如 0.5',
        hint: '请求带 `tools` 时增加的权重。',
      },
      {
        key: 'thinkingBonus',
        label: 'Thinking 加权',
        step: '0.1',
        min: '0',
        placeholder: '例如 0.5',
        hint: '请求启用 thinking 时增加的权重。',
      },
      {
        key: 'heavyThinkingBudgetThreshold',
        label: '重 thinking 阈值',
        step: '1',
        min: '0',
        placeholder: '例如 16000',
        hint: 'thinking budget 超过此值时，判定为更重的请求。',
      },
      {
        key: 'heavyThinkingBudgetBonus',
        label: '重 thinking 加权',
        step: '0.1',
        min: '0',
        placeholder: '例如 0.5',
        hint: '命中重 thinking 阈值后额外增加的权重。',
      },
    ],
  },
]

function createRequestWeightingInputs(config: RequestWeightingConfig): RequestWeightingInputState {
  return {
    baseWeight: String(config.baseWeight),
    maxWeight: String(config.maxWeight),
    toolsBonus: String(config.toolsBonus),
    largeMaxTokensThreshold: String(config.largeMaxTokensThreshold),
    largeMaxTokensBonus: String(config.largeMaxTokensBonus),
    largeInputTokensThreshold: String(config.largeInputTokensThreshold),
    largeInputTokensBonus: String(config.largeInputTokensBonus),
    veryLargeInputTokensThreshold: String(config.veryLargeInputTokensThreshold),
    veryLargeInputTokensBonus: String(config.veryLargeInputTokensBonus),
    thinkingBonus: String(config.thinkingBonus),
    heavyThinkingBudgetThreshold: String(config.heavyThinkingBudgetThreshold),
    heavyThinkingBudgetBonus: String(config.heavyThinkingBudgetBonus),
  }
}

export function SettingsPage() {
  const { data: loadBalancingData, isLoading: isLoadingMode } = useLoadBalancingMode()
  const { mutate: setLoadBalancingMode, isPending: isSettingMode } = useSetLoadBalancingMode()
  const { data: modelCapabilitiesData, isLoading: isLoadingCapabilities } = useModelCapabilitiesConfig()
  const { mutate: setModelCapabilitiesConfig, isPending: isSettingCapabilities } =
    useSetModelCapabilitiesConfig()

  const [queueMaxSizeInput, setQueueMaxSizeInput] = useState('0')
  const [queueMaxWaitMsInput, setQueueMaxWaitMsInput] = useState('0')
  const [rateLimitCooldownMsInput, setRateLimitCooldownMsInput] = useState('2000')
  const [defaultMaxConcurrencyInput, setDefaultMaxConcurrencyInput] = useState('')
  const [rateLimitBucketCapacityInput, setRateLimitBucketCapacityInput] = useState('6')
  const [rateLimitRefillPerSecondInput, setRateLimitRefillPerSecondInput] = useState('2')
  const [rateLimitRefillMinPerSecondInput, setRateLimitRefillMinPerSecondInput] = useState('1')
  const [rateLimitRefillRecoveryStepInput, setRateLimitRefillRecoveryStepInput] = useState('0.25')
  const [rateLimitRefillBackoffFactorInput, setRateLimitRefillBackoffFactorInput] = useState('0.75')
  const [requestWeightingEnabled, setRequestWeightingEnabled] = useState(DEFAULT_REQUEST_WEIGHTING.enabled)
  const [requestWeightingInputs, setRequestWeightingInputs] = useState<RequestWeightingInputState>(
    () => createRequestWeightingInputs(DEFAULT_REQUEST_WEIGHTING)
  )
  const [modelCapabilitiesJson, setModelCapabilitiesJson] = useState('{\n  \n}')

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
    const requestWeighting = loadBalancingData.requestWeighting ?? DEFAULT_REQUEST_WEIGHTING
    setRequestWeightingEnabled(requestWeighting.enabled)
    setRequestWeightingInputs(createRequestWeightingInputs(requestWeighting))
  }, [loadBalancingData])

  useEffect(() => {
    if (!modelCapabilitiesData) {
      return
    }
    setModelCapabilitiesJson(
      JSON.stringify(modelCapabilitiesData.accountTypePolicies ?? {}, null, 2)
    )
  }, [modelCapabilitiesData])

  const handleRequestWeightingInputChange = (
    key: RequestWeightingNumericField,
    value: string
  ) => {
    setRequestWeightingInputs((prev) => ({
      ...prev,
      [key]: value,
    }))
  }

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
    const parsedRequestWeighting: RequestWeightingConfig = {
      enabled: requestWeightingEnabled,
      baseWeight: requestWeightingInputs.baseWeight.trim() === '' ? 0 : Number.parseFloat(requestWeightingInputs.baseWeight),
      maxWeight: requestWeightingInputs.maxWeight.trim() === '' ? 0 : Number.parseFloat(requestWeightingInputs.maxWeight),
      toolsBonus: requestWeightingInputs.toolsBonus.trim() === '' ? 0 : Number.parseFloat(requestWeightingInputs.toolsBonus),
      largeMaxTokensThreshold: requestWeightingInputs.largeMaxTokensThreshold.trim() === '' ? 0 : parseInt(requestWeightingInputs.largeMaxTokensThreshold, 10),
      largeMaxTokensBonus: requestWeightingInputs.largeMaxTokensBonus.trim() === '' ? 0 : Number.parseFloat(requestWeightingInputs.largeMaxTokensBonus),
      largeInputTokensThreshold: requestWeightingInputs.largeInputTokensThreshold.trim() === '' ? 0 : parseInt(requestWeightingInputs.largeInputTokensThreshold, 10),
      largeInputTokensBonus: requestWeightingInputs.largeInputTokensBonus.trim() === '' ? 0 : Number.parseFloat(requestWeightingInputs.largeInputTokensBonus),
      veryLargeInputTokensThreshold: requestWeightingInputs.veryLargeInputTokensThreshold.trim() === '' ? 0 : parseInt(requestWeightingInputs.veryLargeInputTokensThreshold, 10),
      veryLargeInputTokensBonus: requestWeightingInputs.veryLargeInputTokensBonus.trim() === '' ? 0 : Number.parseFloat(requestWeightingInputs.veryLargeInputTokensBonus),
      thinkingBonus: requestWeightingInputs.thinkingBonus.trim() === '' ? 0 : Number.parseFloat(requestWeightingInputs.thinkingBonus),
      heavyThinkingBudgetThreshold: requestWeightingInputs.heavyThinkingBudgetThreshold.trim() === '' ? 0 : parseInt(requestWeightingInputs.heavyThinkingBudgetThreshold, 10),
      heavyThinkingBudgetBonus: requestWeightingInputs.heavyThinkingBudgetBonus.trim() === '' ? 0 : Number.parseFloat(requestWeightingInputs.heavyThinkingBudgetBonus),
    }

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

    const requestWeightingValues = [
      parsedRequestWeighting.baseWeight,
      parsedRequestWeighting.maxWeight,
      parsedRequestWeighting.toolsBonus,
      parsedRequestWeighting.largeMaxTokensThreshold,
      parsedRequestWeighting.largeMaxTokensBonus,
      parsedRequestWeighting.largeInputTokensThreshold,
      parsedRequestWeighting.largeInputTokensBonus,
      parsedRequestWeighting.veryLargeInputTokensThreshold,
      parsedRequestWeighting.veryLargeInputTokensBonus,
      parsedRequestWeighting.thinkingBonus,
      parsedRequestWeighting.heavyThinkingBudgetThreshold,
      parsedRequestWeighting.heavyThinkingBudgetBonus,
    ]

    if (
      requestWeightingValues.some((value) => Number.isNaN(value) || value < 0)
    ) {
      toast.error('轻/重请求加权参数必须是大于等于 0 的数字')
      return
    }

    if (parsedRequestWeighting.baseWeight <= 0) {
      toast.error('基础权重必须大于 0')
      return
    }

    if (parsedRequestWeighting.maxWeight < parsedRequestWeighting.baseWeight) {
      toast.error('最大权重不能小于基础权重')
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
        requestWeighting: parsedRequestWeighting,
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

  const handleSaveModelCapabilities = () => {
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
          setModelCapabilitiesJson(JSON.stringify(response.accountTypePolicies ?? {}, null, 2))
          toast.success('账号类型模型策略已更新')
        },
        onError: (error) => {
          toast.error(`保存失败: ${extractErrorMessage(error)}`)
        },
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

          <div className="space-y-4 md:col-span-2">
            <h3 className="text-sm font-semibold flex items-center text-primary">轻 / 重请求加权</h3>
            <div className="space-y-4 rounded-lg bg-muted/30 p-4">
              <div className="flex flex-col gap-4 rounded-lg border bg-background/70 p-4 md:flex-row md:items-center md:justify-between">
                <div className="space-y-1">
                  <div className="text-sm font-medium">按请求复杂度动态消耗 bucket</div>
                  <p className="text-sm text-muted-foreground">
                    适配“轻请求 / 重代码请求”混跑。启用后，`tools`、`thinking`、大输入和高 `maxTokens`
                    请求会消耗更多本地 bucket 配额。
                  </p>
                </div>
                <div className="flex items-center gap-3">
                  <Badge variant={requestWeightingEnabled ? 'secondary' : 'outline'}>
                    {requestWeightingEnabled ? '已启用' : '已禁用'}
                  </Badge>
                  <Switch
                    checked={requestWeightingEnabled}
                    onCheckedChange={setRequestWeightingEnabled}
                    aria-label="切换轻重请求加权"
                  />
                </div>
              </div>

              {REQUEST_WEIGHTING_FIELD_SECTIONS.map((section) => (
                <div key={section.title} className="space-y-3 rounded-lg border bg-background/50 p-4">
                  <div className="space-y-1">
                    <div className="text-sm font-medium">{section.title}</div>
                    <p className="text-sm text-muted-foreground">{section.description}</p>
                  </div>
                  <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                    {section.fields.map((field) => (
                      <div key={field.key} className="space-y-2">
                        <label className="text-sm font-medium" htmlFor={field.key}>
                          {field.label}
                        </label>
                        <Input
                          id={field.key}
                          type="number"
                          min={field.min}
                          step={field.step}
                          value={requestWeightingInputs[field.key]}
                          onChange={(e) => handleRequestWeightingInputChange(field.key, e.target.value)}
                          placeholder={field.placeholder}
                        />
                        <p className="text-xs text-muted-foreground">{field.hint}</p>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </CardContent>

        <div className="px-6 py-4 bg-muted/20 border-t flex items-center justify-between flex-wrap gap-4 rounded-b-lg">
          <div className="flex text-sm text-muted-foreground max-w-2xl gap-2">
            <AlertCircle className="h-5 w-5 shrink-0 text-yellow-600 dark:text-yellow-500" />
            <p>
              `defaultMaxConcurrency` 是未单独设置账号维度的默认回退并发上限。当前推荐组合是
              <code className="bg-muted px-1 rounded">queueMaxWaitMs=5000</code>、
              <code className="bg-muted px-1 rounded">rateLimitBucketCapacity=6</code>、
              <code className="bg-muted px-1 rounded">rateLimitRefillPerSecond=2</code>、
              <code className="bg-muted px-1 rounded">rateLimitRefillMinPerSecond=1</code>，
              再配合上面的 `requestWeighting` 让重代码/重 thinking 请求多消耗一些 bucket。
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

      <Card className="border-muted shadow-sm">
        <CardHeader>
          <CardTitle>账号类型模型策略</CardTitle>
          <CardDescription>
            用 JSON 维护“账号类型 → 默认允许/禁用模型”映射。账号级策略会在这里的基础上继续叠加。
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="rounded-lg bg-muted/30 p-4">
            <div className="mb-2 flex items-center justify-between gap-2">
              <div className="text-sm font-medium">`accountTypePolicies` JSON</div>
              {isLoadingCapabilities && (
                <Badge variant="outline">加载中</Badge>
              )}
            </div>
            <textarea
              rows={14}
              value={modelCapabilitiesJson}
              onChange={(e) => setModelCapabilitiesJson(e.target.value)}
              spellCheck={false}
              className="flex min-h-[280px] w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
              placeholder={`{\n  "power": {\n    "allowedModels": ["claude-opus-4.6"],\n    "blockedModels": ["claude-opus-4.7"]\n  }\n}`}
            />
            <p className="mt-2 text-xs text-muted-foreground">
              键是账号类型名，值支持 `allowedModels` 和 `blockedModels`。允许列表为空表示不限制，拒绝列表始终优先。
            </p>
          </div>
          <div className="flex items-center justify-between gap-4">
            <p className="text-sm text-muted-foreground">
              建议把稳定的账号池差异放在这里，把单个例外账号留到凭据卡片里单独覆盖。
            </p>
            <Button onClick={handleSaveModelCapabilities} disabled={isSettingCapabilities}>
              <Save className="h-4 w-4 mr-2" />
              保存账号类型策略
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
