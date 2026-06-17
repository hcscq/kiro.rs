import { useEffect, useState, type ReactNode } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { toast } from 'sonner'
import {
  useLoadBalancingMode,
  useSetLoadBalancingMode,
} from '@/hooks/use-credentials'
import { extractErrorMessage } from '@/lib/utils'
import { Save, AlertCircle, Info, Plus, Trash2 } from 'lucide-react'
import type {
  KiroRequestBodyGuardConfig,
  NonStreamBodyReadTimeoutConfig,
  ProxyPoolConfig,
  ProxyPoolEntry,
  RequestWeightingConfig,
  StreamPreSseFailoverConfig,
  ThinkingSignatureValidationMode,
} from '@/types/api'

type RequestWeightingNumericField = Exclude<keyof RequestWeightingConfig, 'enabled'>
type StreamPreSseFailoverNumericField = Exclude<keyof StreamPreSseFailoverConfig, 'enabled'>

type RequestWeightingInputState = Record<RequestWeightingNumericField, string>
type StreamPreSseFailoverInputState = Record<StreamPreSseFailoverNumericField, string>

const MIB_BYTES = 1024 * 1024

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

const DEFAULT_STREAM_PRE_SSE_FAILOVER: StreamPreSseFailoverConfig = {
  enabled: true,
  totalBudgetMs: 170000,
  smallRequestThresholdBytes: 128 * 1024,
  mediumRequestThresholdBytes: 1024 * 1024,
  largeRequestThresholdBytes: 5 * 1024 * 1024,
  smallRequestTimeoutMs: 30000,
  mediumRequestTimeoutMs: 60000,
  largeRequestTimeoutMs: 120000,
  hugeRequestTimeoutMs: 0,
  slowModelMinTimeoutMs: 60000,
  maxFastFailovers: 2,
  minRemainingMs: 15000,
}

const DEFAULT_NON_STREAM_BODY_READ_TIMEOUT: NonStreamBodyReadTimeoutConfig = {
  enabled: true,
  timeoutMs: 540000,
  eventstreamIdleTimeoutMs: 120000,
  retryOnTimeout: false,
  eventstreamSafeRetryOnStall: true,
}

const DEFAULT_KIRO_REQUEST_BODY_GUARD: KiroRequestBodyGuardConfig = {
  enabled: true,
  maxBytes: 30 * MIB_BYTES,
}

const DEFAULT_PROXY_POOL: ProxyPoolConfig = {
  enabled: false,
  requireProxy: false,
  assignmentStrategy: 'weighted_least_assigned',
  proxies: [],
  failover: {
    enabled: true,
    failureThreshold: 3,
    cooldownSecs: 300,
    probeUrl: null,
  },
}

const THINKING_SIGNATURE_VALIDATION_OPTIONS: Array<{
  value: ThinkingSignatureValidationMode
  label: string
  description: string
}> = [
  {
    value: 'strict',
    label: '严格拒绝',
    description: '校验失败直接拒绝请求，保持默认安全边界。',
  },
  {
    value: 'warn_only',
    label: '只告警放行',
    description: '记录异常诊断日志，但继续转发到上游。',
  },
  {
    value: 'strip_invalid',
    label: '剥离后放行',
    description: '移除本服务签发但失效的签名，再继续转发。',
  },
  {
    value: 'disabled',
    label: '关闭校验',
    description: '完全跳过本地 thinking signature 校验。',
  },
]

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

const STREAM_PRE_SSE_FAILOVER_FIELDS: Array<{
  key: StreamPreSseFailoverNumericField
  label: string
  step: string
  min: string
  placeholder: string
  hint: string
}> = [
  {
    key: 'totalBudgetMs',
    label: '总等待预算 (毫秒)',
    step: '1000',
    min: '1',
    placeholder: '默认 170000',
    hint: '整个流式请求在收到上游响应头前最多等待的总时长。',
  },
  {
    key: 'smallRequestThresholdBytes',
    label: '小请求阈值 (bytes)',
    step: '1024',
    min: '1',
    placeholder: '默认 131072',
    hint: '请求体不超过该值时使用小请求超时。',
  },
  {
    key: 'mediumRequestThresholdBytes',
    label: '中请求阈值 (bytes)',
    step: '1024',
    min: '1',
    placeholder: '默认 1048576',
    hint: '请求体不超过该值时使用中请求超时。',
  },
  {
    key: 'largeRequestThresholdBytes',
    label: '大请求阈值 (bytes)',
    step: '1024',
    min: '1',
    placeholder: '默认 5242880',
    hint: '请求体不超过该值时使用大请求超时，超过后进入超大请求档。',
  },
  {
    key: 'smallRequestTimeoutMs',
    label: '小请求单次等待 (毫秒)',
    step: '1000',
    min: '1',
    placeholder: '默认 30000',
    hint: '小请求单个凭据等待响应头的时间。',
  },
  {
    key: 'mediumRequestTimeoutMs',
    label: '中请求单次等待 (毫秒)',
    step: '1000',
    min: '1',
    placeholder: '默认 60000',
    hint: '中请求单个凭据等待响应头的时间。',
  },
  {
    key: 'largeRequestTimeoutMs',
    label: '大请求单次等待 (毫秒)',
    step: '1000',
    min: '1',
    placeholder: '默认 120000',
    hint: '大请求单个凭据等待响应头的时间。',
  },
  {
    key: 'hugeRequestTimeoutMs',
    label: '超大请求单次等待 (毫秒)',
    step: '1000',
    min: '0',
    placeholder: '0 表示使用剩余总预算',
    hint: '设为 0 时超大请求不做快速故障转移。',
  },
  {
    key: 'slowModelMinTimeoutMs',
    label: '高阶 Opus 最小等待 (毫秒)',
    step: '1000',
    min: '1',
    placeholder: '默认 60000',
    hint: '真实 Opus 4.7/4.8 请求的单次等待不会低于该值。',
  },
  {
    key: 'maxFastFailovers',
    label: '最大快速切换次数',
    step: '1',
    min: '0',
    placeholder: '默认 2',
    hint: '超过该次数后，最后一次尝试会使用剩余总预算。',
  },
  {
    key: 'minRemainingMs',
    label: '保留剩余预算 (毫秒)',
    step: '1000',
    min: '1',
    placeholder: '默认 15000',
    hint: '只有预计切换后仍保留该预算时，才会提前切换凭据。',
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

function createStreamPreSseFailoverInputs(
  config: StreamPreSseFailoverConfig
): StreamPreSseFailoverInputState {
  return {
    totalBudgetMs: String(config.totalBudgetMs),
    smallRequestThresholdBytes: String(config.smallRequestThresholdBytes),
    mediumRequestThresholdBytes: String(config.mediumRequestThresholdBytes),
    largeRequestThresholdBytes: String(config.largeRequestThresholdBytes),
    smallRequestTimeoutMs: String(config.smallRequestTimeoutMs),
    mediumRequestTimeoutMs: String(config.mediumRequestTimeoutMs),
    largeRequestTimeoutMs: String(config.largeRequestTimeoutMs),
    hugeRequestTimeoutMs: String(config.hugeRequestTimeoutMs),
    slowModelMinTimeoutMs: String(config.slowModelMinTimeoutMs),
    maxFastFailovers: String(config.maxFastFailovers),
    minRemainingMs: String(config.minRemainingMs),
  }
}

function bytesToMiBInput(bytes: number): string {
  const value = bytes / MIB_BYTES
  return value.toFixed(2).replace(/\.?0+$/, '')
}

function SettingInfoTooltip({
  label,
  children,
}: {
  label: string
  children: ReactNode
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          className="inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-full text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
          aria-label={`${label}说明`}
        >
          <Info className="h-4 w-4" />
        </button>
      </TooltipTrigger>
      <TooltipContent side="top" align="start">
        <div className="space-y-1 leading-relaxed">{children}</div>
      </TooltipContent>
    </Tooltip>
  )
}

function SwitchSettingCard({
  title,
  description,
  checked,
  onCheckedChange,
  ariaLabel,
  disabledLabel = '已关闭',
  warning,
}: {
  title: string
  description: ReactNode
  checked: boolean
  onCheckedChange: (checked: boolean) => void
  ariaLabel: string
  disabledLabel?: string
  warning?: string
}) {
  return (
    <div className="flex flex-col gap-3 rounded-lg border bg-background/70 p-4">
      <div className="flex items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2">
          <div className="text-sm font-medium">{title}</div>
          <SettingInfoTooltip label={title}>{description}</SettingInfoTooltip>
        </div>
        <div className="flex shrink-0 items-center gap-3">
          <Badge variant={checked ? 'secondary' : 'outline'}>
            {checked ? '已启用' : disabledLabel}
          </Badge>
          <Switch
            checked={checked}
            onCheckedChange={onCheckedChange}
            aria-label={ariaLabel}
          />
        </div>
      </div>
      {warning && <p className="text-xs text-muted-foreground">{warning}</p>}
    </div>
  )
}

export function SettingsPage() {
  const { data: loadBalancingData, isLoading: isLoadingMode } = useLoadBalancingMode()
  const { mutate: setLoadBalancingMode, isPending: isSettingMode } = useSetLoadBalancingMode()

  const [queueMaxSizeInput, setQueueMaxSizeInput] = useState('0')
  const [queueMaxWaitMsInput, setQueueMaxWaitMsInput] = useState('0')
  const [rateLimitCooldownMsInput, setRateLimitCooldownMsInput] = useState('2000')
  const [rateLimitCooldownEnabled, setRateLimitCooldownEnabled] = useState(false)
  const [suspiciousActivityCooldownMsInput, setSuspiciousActivityCooldownMsInput] = useState('7200000')
  const [suspiciousActivityCooldownEnabled, setSuspiciousActivityCooldownEnabled] = useState(true)
  const [suspiciousActivityPreferCleanCredentials, setSuspiciousActivityPreferCleanCredentials] = useState(true)
  const [suspiciousActivityAutoDisableEnabled, setSuspiciousActivityAutoDisableEnabled] = useState(true)
  const [suspiciousActivityAutoDisableThresholdInput, setSuspiciousActivityAutoDisableThresholdInput] = useState('3')
  const [suspiciousActivityAutoDisableWindowMsInput, setSuspiciousActivityAutoDisableWindowMsInput] = useState('86400000')
  const [suspiciousActivityAutoClearEnabled, setSuspiciousActivityAutoClearEnabled] = useState(true)
  const [suspiciousActivityAutoClearSuccessThresholdInput, setSuspiciousActivityAutoClearSuccessThresholdInput] = useState('10')
  const [suspiciousActivityAutoClearAfterMsInput, setSuspiciousActivityAutoClearAfterMsInput] = useState('604800000')
  const [modelCooldownEnabled, setModelCooldownEnabled] = useState(false)
  const [sessionAffinityEnabled, setSessionAffinityEnabled] = useState(false)
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
  const [streamDispatchLeaseReleaseEnabled, setStreamDispatchLeaseReleaseEnabled] =
    useState(true)
  const [streamPreSseFailoverEnabled, setStreamPreSseFailoverEnabled] =
    useState(DEFAULT_STREAM_PRE_SSE_FAILOVER.enabled)
  const [streamPreSseFailoverInputs, setStreamPreSseFailoverInputs] =
    useState<StreamPreSseFailoverInputState>(
      () => createStreamPreSseFailoverInputs(DEFAULT_STREAM_PRE_SSE_FAILOVER)
    )
  const [nonStreamBodyReadTimeoutEnabled, setNonStreamBodyReadTimeoutEnabled] =
    useState(DEFAULT_NON_STREAM_BODY_READ_TIMEOUT.enabled)
  const [nonStreamBodyReadTimeoutMsInput, setNonStreamBodyReadTimeoutMsInput] =
    useState(String(DEFAULT_NON_STREAM_BODY_READ_TIMEOUT.timeoutMs))
  const [nonStreamEventstreamIdleTimeoutMsInput, setNonStreamEventstreamIdleTimeoutMsInput] =
    useState(String(DEFAULT_NON_STREAM_BODY_READ_TIMEOUT.eventstreamIdleTimeoutMs))
  const [nonStreamBodyReadTimeoutRetryOnTimeout, setNonStreamBodyReadTimeoutRetryOnTimeout] =
    useState(DEFAULT_NON_STREAM_BODY_READ_TIMEOUT.retryOnTimeout)
  const [nonStreamEventstreamSafeRetryOnStall, setNonStreamEventstreamSafeRetryOnStall] =
    useState(DEFAULT_NON_STREAM_BODY_READ_TIMEOUT.eventstreamSafeRetryOnStall)
  const [kiroRequestBodyGuardEnabled, setKiroRequestBodyGuardEnabled] =
    useState(DEFAULT_KIRO_REQUEST_BODY_GUARD.enabled)
  const [kiroRequestBodyGuardMaxMiBInput, setKiroRequestBodyGuardMaxMiBInput] =
    useState(bytesToMiBInput(DEFAULT_KIRO_REQUEST_BODY_GUARD.maxBytes))
  const [thinkingSignatureValidationMode, setThinkingSignatureValidationMode] =
    useState<ThinkingSignatureValidationMode>('strict')
  const [responseThinkingSignatureCompatEnabled, setResponseThinkingSignatureCompatEnabled] =
    useState(false)
  const [proxyPool, setProxyPool] = useState<ProxyPoolConfig>(DEFAULT_PROXY_POOL)

  useEffect(() => {
    if (!loadBalancingData) {
      return
    }
    setQueueMaxSizeInput(String(loadBalancingData.queueMaxSize))
    setQueueMaxWaitMsInput(String(loadBalancingData.queueMaxWaitMs))
    setRateLimitCooldownMsInput(String(loadBalancingData.rateLimitCooldownMs))
    setRateLimitCooldownEnabled(loadBalancingData.rateLimitCooldownEnabled ?? false)
    setSuspiciousActivityCooldownMsInput(String(loadBalancingData.suspiciousActivityCooldownMs ?? 7200000))
    setSuspiciousActivityCooldownEnabled(loadBalancingData.suspiciousActivityCooldownEnabled ?? true)
    setSuspiciousActivityPreferCleanCredentials(loadBalancingData.suspiciousActivityPreferCleanCredentials ?? true)
    setSuspiciousActivityAutoDisableEnabled(loadBalancingData.suspiciousActivityAutoDisableEnabled ?? true)
    setSuspiciousActivityAutoDisableThresholdInput(String(loadBalancingData.suspiciousActivityAutoDisableThreshold ?? 3))
    setSuspiciousActivityAutoDisableWindowMsInput(String(loadBalancingData.suspiciousActivityAutoDisableWindowMs ?? 86400000))
    setSuspiciousActivityAutoClearEnabled(loadBalancingData.suspiciousActivityAutoClearEnabled ?? true)
    setSuspiciousActivityAutoClearSuccessThresholdInput(String(loadBalancingData.suspiciousActivityAutoClearSuccessThreshold ?? 10))
    setSuspiciousActivityAutoClearAfterMsInput(String(loadBalancingData.suspiciousActivityAutoClearAfterMs ?? 604800000))
    setModelCooldownEnabled(loadBalancingData.modelCooldownEnabled ?? true)
    setSessionAffinityEnabled(loadBalancingData.sessionAffinityEnabled ?? false)
    setDefaultMaxConcurrencyInput(loadBalancingData.defaultMaxConcurrency ? String(loadBalancingData.defaultMaxConcurrency) : '')
    setRateLimitBucketCapacityInput(String(loadBalancingData.rateLimitBucketCapacity))
    setRateLimitRefillPerSecondInput(String(loadBalancingData.rateLimitRefillPerSecond))
    setRateLimitRefillMinPerSecondInput(String(loadBalancingData.rateLimitRefillMinPerSecond))
    setRateLimitRefillRecoveryStepInput(String(loadBalancingData.rateLimitRefillRecoveryStepPerSuccess))
    setRateLimitRefillBackoffFactorInput(String(loadBalancingData.rateLimitRefillBackoffFactor))
    const requestWeighting = loadBalancingData.requestWeighting ?? DEFAULT_REQUEST_WEIGHTING
    setRequestWeightingEnabled(requestWeighting.enabled)
    setRequestWeightingInputs(createRequestWeightingInputs(requestWeighting))
    setStreamDispatchLeaseReleaseEnabled(loadBalancingData.streamDispatchLeaseReleaseEnabled ?? true)
    const streamPreSseFailover =
      loadBalancingData.streamPreSseFailover ?? DEFAULT_STREAM_PRE_SSE_FAILOVER
    setStreamPreSseFailoverEnabled(streamPreSseFailover.enabled)
    setStreamPreSseFailoverInputs(createStreamPreSseFailoverInputs(streamPreSseFailover))
    const nonStreamBodyReadTimeout = {
      ...DEFAULT_NON_STREAM_BODY_READ_TIMEOUT,
      ...(loadBalancingData.nonStreamBodyReadTimeout ?? {}),
    }
    setNonStreamBodyReadTimeoutEnabled(nonStreamBodyReadTimeout.enabled)
    setNonStreamBodyReadTimeoutMsInput(String(nonStreamBodyReadTimeout.timeoutMs))
    setNonStreamEventstreamIdleTimeoutMsInput(String(nonStreamBodyReadTimeout.eventstreamIdleTimeoutMs))
    setNonStreamBodyReadTimeoutRetryOnTimeout(nonStreamBodyReadTimeout.retryOnTimeout)
    setNonStreamEventstreamSafeRetryOnStall(nonStreamBodyReadTimeout.eventstreamSafeRetryOnStall)
    const kiroRequestBodyGuard = {
      ...DEFAULT_KIRO_REQUEST_BODY_GUARD,
      ...(loadBalancingData.kiroRequestBodyGuard ?? {}),
    }
    setKiroRequestBodyGuardEnabled(kiroRequestBodyGuard.enabled)
    setKiroRequestBodyGuardMaxMiBInput(bytesToMiBInput(kiroRequestBodyGuard.maxBytes))
    setThinkingSignatureValidationMode(loadBalancingData.thinkingSignatureValidationMode ?? 'strict')
    setResponseThinkingSignatureCompatEnabled(loadBalancingData.responseThinkingSignatureCompatEnabled ?? false)
    setProxyPool({
      ...DEFAULT_PROXY_POOL,
      ...(loadBalancingData.proxyPool ?? {}),
      failover: {
        ...DEFAULT_PROXY_POOL.failover,
        ...(loadBalancingData.proxyPool?.failover ?? {}),
      },
      proxies: loadBalancingData.proxyPool?.proxies ?? [],
    })
  }, [loadBalancingData])

  const updateProxyPool = (patch: Partial<ProxyPoolConfig>) => {
    setProxyPool((prev) => ({
      ...prev,
      ...patch,
    }))
  }

  const updateProxyPoolFailover = (patch: Partial<ProxyPoolConfig['failover']>) => {
    setProxyPool((prev) => ({
      ...prev,
      failover: {
        ...prev.failover,
        ...patch,
      },
    }))
  }

  const updateProxyPoolEntry = (index: number, patch: Partial<ProxyPoolEntry>) => {
    setProxyPool((prev) => ({
      ...prev,
      proxies: prev.proxies.map((entry, entryIndex) =>
        entryIndex === index ? { ...entry, ...patch } : entry
      ),
    }))
  }

  const addProxyPoolEntry = () => {
    setProxyPool((prev) => ({
      ...prev,
      proxies: [
        ...prev.proxies,
        {
          id: `proxy-${prev.proxies.length + 1}`,
          url: '',
          username: null,
          password: null,
          weight: 1,
          enabled: true,
          expectedEgressIp: null,
        },
      ],
    }))
  }

  const removeProxyPoolEntry = (index: number) => {
    setProxyPool((prev) => ({
      ...prev,
      proxies: prev.proxies.filter((_, entryIndex) => entryIndex !== index),
    }))
  }

  const handleRequestWeightingInputChange = (
    key: RequestWeightingNumericField,
    value: string
  ) => {
    setRequestWeightingInputs((prev) => ({
      ...prev,
      [key]: value,
    }))
  }

  const handleStreamPreSseFailoverInputChange = (
    key: StreamPreSseFailoverNumericField,
    value: string
  ) => {
    setStreamPreSseFailoverInputs((prev) => ({
      ...prev,
      [key]: value,
    }))
  }

  const handleSaveQueueSettings = () => {
    const parsedQueueMaxSize = queueMaxSizeInput.trim() === '' ? 0 : parseInt(queueMaxSizeInput, 10)
    const parsedQueueMaxWaitMs = queueMaxWaitMsInput.trim() === '' ? 0 : parseInt(queueMaxWaitMsInput, 10)
    const parsedRateLimitCooldownMs = rateLimitCooldownMsInput.trim() === '' ? 0 : parseInt(rateLimitCooldownMsInput, 10)
    const parsedSuspiciousActivityCooldownMs = suspiciousActivityCooldownMsInput.trim() === '' ? 0 : parseInt(suspiciousActivityCooldownMsInput, 10)
    const parsedSuspiciousActivityAutoDisableThreshold = suspiciousActivityAutoDisableThresholdInput.trim() === '' ? 0 : parseInt(suspiciousActivityAutoDisableThresholdInput, 10)
    const parsedSuspiciousActivityAutoDisableWindowMs = suspiciousActivityAutoDisableWindowMsInput.trim() === '' ? 0 : parseInt(suspiciousActivityAutoDisableWindowMsInput, 10)
    const parsedSuspiciousActivityAutoClearSuccessThreshold = suspiciousActivityAutoClearSuccessThresholdInput.trim() === '' ? 0 : parseInt(suspiciousActivityAutoClearSuccessThresholdInput, 10)
    const parsedSuspiciousActivityAutoClearAfterMs = suspiciousActivityAutoClearAfterMsInput.trim() === '' ? 0 : parseInt(suspiciousActivityAutoClearAfterMsInput, 10)
    const parsedDefaultMaxConcurrency = defaultMaxConcurrencyInput.trim() === '' ? 0 : parseInt(defaultMaxConcurrencyInput, 10)
    const parsedRateLimitBucketCapacity = rateLimitBucketCapacityInput.trim() === '' ? 0 : Number.parseFloat(rateLimitBucketCapacityInput)
    const parsedRateLimitRefillPerSecond = rateLimitRefillPerSecondInput.trim() === '' ? 0 : Number.parseFloat(rateLimitRefillPerSecondInput)
    const parsedRateLimitRefillMinPerSecond = rateLimitRefillMinPerSecondInput.trim() === '' ? 0 : Number.parseFloat(rateLimitRefillMinPerSecondInput)
    const parsedRateLimitRefillRecoveryStep = rateLimitRefillRecoveryStepInput.trim() === '' ? 0 : Number.parseFloat(rateLimitRefillRecoveryStepInput)
    const parsedRateLimitRefillBackoffFactor = rateLimitRefillBackoffFactorInput.trim() === '' ? 0 : Number.parseFloat(rateLimitRefillBackoffFactorInput)
    const parsedNonStreamBodyReadTimeoutMs = nonStreamBodyReadTimeoutMsInput.trim() === '' ? 0 : parseInt(nonStreamBodyReadTimeoutMsInput, 10)
    const parsedNonStreamEventstreamIdleTimeoutMs = nonStreamEventstreamIdleTimeoutMsInput.trim() === '' ? 0 : parseInt(nonStreamEventstreamIdleTimeoutMsInput, 10)
    const parsedKiroRequestBodyGuardMaxMiB = kiroRequestBodyGuardMaxMiBInput.trim() === '' ? 0 : Number.parseFloat(kiroRequestBodyGuardMaxMiBInput)
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
    const parsedStreamPreSseFailover: StreamPreSseFailoverConfig = {
      enabled: streamPreSseFailoverEnabled,
      totalBudgetMs: streamPreSseFailoverInputs.totalBudgetMs.trim() === '' ? 0 : parseInt(streamPreSseFailoverInputs.totalBudgetMs, 10),
      smallRequestThresholdBytes: streamPreSseFailoverInputs.smallRequestThresholdBytes.trim() === '' ? 0 : parseInt(streamPreSseFailoverInputs.smallRequestThresholdBytes, 10),
      mediumRequestThresholdBytes: streamPreSseFailoverInputs.mediumRequestThresholdBytes.trim() === '' ? 0 : parseInt(streamPreSseFailoverInputs.mediumRequestThresholdBytes, 10),
      largeRequestThresholdBytes: streamPreSseFailoverInputs.largeRequestThresholdBytes.trim() === '' ? 0 : parseInt(streamPreSseFailoverInputs.largeRequestThresholdBytes, 10),
      smallRequestTimeoutMs: streamPreSseFailoverInputs.smallRequestTimeoutMs.trim() === '' ? 0 : parseInt(streamPreSseFailoverInputs.smallRequestTimeoutMs, 10),
      mediumRequestTimeoutMs: streamPreSseFailoverInputs.mediumRequestTimeoutMs.trim() === '' ? 0 : parseInt(streamPreSseFailoverInputs.mediumRequestTimeoutMs, 10),
      largeRequestTimeoutMs: streamPreSseFailoverInputs.largeRequestTimeoutMs.trim() === '' ? 0 : parseInt(streamPreSseFailoverInputs.largeRequestTimeoutMs, 10),
      hugeRequestTimeoutMs: streamPreSseFailoverInputs.hugeRequestTimeoutMs.trim() === '' ? 0 : parseInt(streamPreSseFailoverInputs.hugeRequestTimeoutMs, 10),
      slowModelMinTimeoutMs: streamPreSseFailoverInputs.slowModelMinTimeoutMs.trim() === '' ? 0 : parseInt(streamPreSseFailoverInputs.slowModelMinTimeoutMs, 10),
      maxFastFailovers: streamPreSseFailoverInputs.maxFastFailovers.trim() === '' ? 0 : parseInt(streamPreSseFailoverInputs.maxFastFailovers, 10),
      minRemainingMs: streamPreSseFailoverInputs.minRemainingMs.trim() === '' ? 0 : parseInt(streamPreSseFailoverInputs.minRemainingMs, 10),
    }
    const parsedProxyPool: ProxyPoolConfig = {
      enabled: proxyPool.enabled,
      requireProxy: proxyPool.requireProxy,
      assignmentStrategy: proxyPool.assignmentStrategy,
      proxies: proxyPool.proxies.map((entry) => ({
        id: entry.id.trim(),
        url: entry.url.trim(),
        username: entry.username?.trim() || null,
        password: entry.password?.trim() || null,
        weight: Number(entry.weight),
        enabled: entry.enabled,
        expectedEgressIp: entry.expectedEgressIp?.trim() || null,
      })),
      failover: {
        enabled: proxyPool.failover.enabled,
        failureThreshold: Number(proxyPool.failover.failureThreshold),
        cooldownSecs: Number(proxyPool.failover.cooldownSecs),
        probeUrl: proxyPool.failover.probeUrl?.trim() || null,
      },
    }

    if (
      Number.isNaN(parsedQueueMaxSize) ||
      Number.isNaN(parsedQueueMaxWaitMs) ||
      Number.isNaN(parsedRateLimitCooldownMs) ||
      Number.isNaN(parsedSuspiciousActivityCooldownMs) ||
      Number.isNaN(parsedSuspiciousActivityAutoDisableThreshold) ||
      Number.isNaN(parsedSuspiciousActivityAutoDisableWindowMs) ||
      Number.isNaN(parsedSuspiciousActivityAutoClearSuccessThreshold) ||
      Number.isNaN(parsedSuspiciousActivityAutoClearAfterMs) ||
      Number.isNaN(parsedDefaultMaxConcurrency) ||
      Number.isNaN(parsedRateLimitBucketCapacity) ||
      Number.isNaN(parsedRateLimitRefillPerSecond) ||
      Number.isNaN(parsedRateLimitRefillMinPerSecond) ||
      Number.isNaN(parsedRateLimitRefillRecoveryStep) ||
      Number.isNaN(parsedRateLimitRefillBackoffFactor) ||
      Number.isNaN(parsedNonStreamBodyReadTimeoutMs) ||
      Number.isNaN(parsedNonStreamEventstreamIdleTimeoutMs) ||
      Number.isNaN(parsedKiroRequestBodyGuardMaxMiB) ||
      parsedQueueMaxSize < 0 ||
      parsedQueueMaxWaitMs < 0 ||
      parsedRateLimitCooldownMs < 0 ||
      parsedSuspiciousActivityCooldownMs < 0 ||
      parsedSuspiciousActivityAutoDisableThreshold < 0 ||
      parsedSuspiciousActivityAutoDisableWindowMs < 0 ||
      parsedSuspiciousActivityAutoClearSuccessThreshold < 0 ||
      parsedSuspiciousActivityAutoClearAfterMs < 0 ||
      parsedDefaultMaxConcurrency < 0 ||
      parsedRateLimitBucketCapacity < 0 ||
      parsedRateLimitRefillPerSecond < 0 ||
      parsedRateLimitRefillMinPerSecond < 0 ||
      parsedRateLimitRefillRecoveryStep < 0 ||
      parsedNonStreamBodyReadTimeoutMs < 0 ||
      parsedNonStreamEventstreamIdleTimeoutMs < 0 ||
      parsedKiroRequestBodyGuardMaxMiB < 0
    ) {
      toast.error('调度参数必须是大于等于 0 的数字')
      return
    }

    if (suspiciousActivityAutoDisableEnabled && parsedSuspiciousActivityAutoDisableThreshold <= 0) {
      toast.error('Suspicious 自动停调阈值必须大于 0')
      return
    }

    if (
      suspiciousActivityAutoClearEnabled &&
      parsedSuspiciousActivityAutoClearSuccessThreshold <= 0 &&
      parsedSuspiciousActivityAutoClearAfterMs <= 0
    ) {
      toast.error('Suspicious 自动恢复需要至少配置成功次数或时间阈值')
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

    const streamPreSseFailoverValues = [
      parsedStreamPreSseFailover.totalBudgetMs,
      parsedStreamPreSseFailover.smallRequestThresholdBytes,
      parsedStreamPreSseFailover.mediumRequestThresholdBytes,
      parsedStreamPreSseFailover.largeRequestThresholdBytes,
      parsedStreamPreSseFailover.smallRequestTimeoutMs,
      parsedStreamPreSseFailover.mediumRequestTimeoutMs,
      parsedStreamPreSseFailover.largeRequestTimeoutMs,
      parsedStreamPreSseFailover.hugeRequestTimeoutMs,
      parsedStreamPreSseFailover.slowModelMinTimeoutMs,
      parsedStreamPreSseFailover.maxFastFailovers,
      parsedStreamPreSseFailover.minRemainingMs,
    ]

    if (
      streamPreSseFailoverValues.some((value) => Number.isNaN(value) || value < 0)
    ) {
      toast.error('流式响应头故障转移参数必须是大于等于 0 的整数')
      return
    }

    if (
      parsedStreamPreSseFailover.totalBudgetMs <= 0 ||
      parsedStreamPreSseFailover.smallRequestThresholdBytes <= 0 ||
      parsedStreamPreSseFailover.smallRequestTimeoutMs <= 0 ||
      parsedStreamPreSseFailover.mediumRequestTimeoutMs <= 0 ||
      parsedStreamPreSseFailover.largeRequestTimeoutMs <= 0 ||
      parsedStreamPreSseFailover.slowModelMinTimeoutMs <= 0 ||
      parsedStreamPreSseFailover.minRemainingMs <= 0
    ) {
      toast.error('流式响应头故障转移参数除超大请求等待和最大切换次数外必须大于 0')
      return
    }

    if (
      parsedStreamPreSseFailover.mediumRequestThresholdBytes <
        parsedStreamPreSseFailover.smallRequestThresholdBytes ||
      parsedStreamPreSseFailover.largeRequestThresholdBytes <
        parsedStreamPreSseFailover.mediumRequestThresholdBytes
    ) {
      toast.error('请求体阈值必须按小、中、大递增')
      return
    }

    if (parsedStreamPreSseFailover.maxFastFailovers > 8) {
      toast.error('最大快速切换次数不能大于 8')
      return
    }

    if (nonStreamBodyReadTimeoutEnabled && parsedNonStreamBodyReadTimeoutMs <= 0) {
      toast.error('非流式响应体读取超时必须大于 0，或关闭启用开关')
      return
    }

    if (nonStreamBodyReadTimeoutEnabled && parsedNonStreamEventstreamIdleTimeoutMs <= 0) {
      toast.error('非流式 EventStream 空闲超时必须大于 0，或关闭启用开关')
      return
    }

    if (
      kiroRequestBodyGuardEnabled &&
      (parsedKiroRequestBodyGuardMaxMiB < 1 || parsedKiroRequestBodyGuardMaxMiB > 64)
    ) {
      toast.error('Kiro 上游 body 上限必须在 1 到 64 MiB 之间，或关闭启用开关')
      return
    }

    if (
      parsedProxyPool.assignmentStrategy !== 'weighted_least_assigned' &&
      parsedProxyPool.assignmentStrategy !== 'hash'
    ) {
      toast.error('代理池分配策略无效')
      return
    }

    const proxyIds = new Set<string>()
    for (const proxy of parsedProxyPool.proxies) {
      if (!proxy.id) {
        toast.error('代理池节点 ID 不能为空')
        return
      }
      if (proxyIds.has(proxy.id)) {
        toast.error(`代理池节点 ID 重复: ${proxy.id}`)
        return
      }
      proxyIds.add(proxy.id)
      if (!proxy.url) {
        toast.error(`代理池节点 ${proxy.id} 的 URL 不能为空`)
        return
      }
      if (!Number.isFinite(proxy.weight) || proxy.weight <= 0) {
        toast.error(`代理池节点 ${proxy.id} 的权重必须大于 0`)
        return
      }
    }

    if (parsedProxyPool.enabled && !parsedProxyPool.proxies.some((proxy) => proxy.enabled)) {
      toast.error('启用代理池时至少需要一个启用节点')
      return
    }

    if (
      parsedProxyPool.failover.enabled &&
      (!Number.isFinite(parsedProxyPool.failover.failureThreshold) ||
        parsedProxyPool.failover.failureThreshold <= 0 ||
        !Number.isFinite(parsedProxyPool.failover.cooldownSecs) ||
        parsedProxyPool.failover.cooldownSecs <= 0)
    ) {
      toast.error('代理故障转移阈值和冷却时间必须大于 0')
      return
    }

    setLoadBalancingMode(
      {
        queueMaxSize: parsedQueueMaxSize,
        queueMaxWaitMs: parsedQueueMaxWaitMs,
        rateLimitCooldownMs: parsedRateLimitCooldownMs,
        rateLimitCooldownEnabled,
        suspiciousActivityCooldownMs: parsedSuspiciousActivityCooldownMs,
        suspiciousActivityCooldownEnabled,
        suspiciousActivityPreferCleanCredentials,
        suspiciousActivityAutoDisableEnabled,
        suspiciousActivityAutoDisableThreshold: parsedSuspiciousActivityAutoDisableThreshold,
        suspiciousActivityAutoDisableWindowMs: parsedSuspiciousActivityAutoDisableWindowMs,
        suspiciousActivityAutoClearEnabled,
        suspiciousActivityAutoClearSuccessThreshold: parsedSuspiciousActivityAutoClearSuccessThreshold,
        suspiciousActivityAutoClearAfterMs: parsedSuspiciousActivityAutoClearAfterMs,
        modelCooldownEnabled,
        sessionAffinityEnabled,
        defaultMaxConcurrency: parsedDefaultMaxConcurrency,
        rateLimitBucketCapacity: parsedRateLimitBucketCapacity,
        rateLimitRefillPerSecond: parsedRateLimitRefillPerSecond,
        rateLimitRefillMinPerSecond: parsedRateLimitRefillMinPerSecond,
        rateLimitRefillRecoveryStepPerSuccess: parsedRateLimitRefillRecoveryStep,
        rateLimitRefillBackoffFactor: parsedRateLimitRefillBackoffFactor,
        requestWeighting: parsedRequestWeighting,
        streamDispatchLeaseReleaseEnabled,
        streamPreSseFailover: parsedStreamPreSseFailover,
        nonStreamBodyReadTimeout: {
          enabled: nonStreamBodyReadTimeoutEnabled,
          timeoutMs: parsedNonStreamBodyReadTimeoutMs,
          eventstreamIdleTimeoutMs: parsedNonStreamEventstreamIdleTimeoutMs,
          retryOnTimeout: nonStreamBodyReadTimeoutRetryOnTimeout,
          eventstreamSafeRetryOnStall: nonStreamEventstreamSafeRetryOnStall,
        },
        kiroRequestBodyGuard: {
          enabled: kiroRequestBodyGuardEnabled,
          maxBytes: Math.round(parsedKiroRequestBodyGuardMaxMiB * MIB_BYTES),
        },
        thinkingSignatureValidationMode,
        responseThinkingSignatureCompatEnabled,
        proxyPool: parsedProxyPool,
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
    <TooltipProvider delayDuration={250}>
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

        <CardContent className="space-y-6">
          <div className="space-y-4">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <h3 className="text-sm font-semibold flex items-center text-primary">代理池配置</h3>
              <Button type="button" variant="outline" size="sm" onClick={addProxyPoolEntry}>
                <Plus className="h-4 w-4" />
                添加节点
              </Button>
            </div>
            <div className="space-y-4 bg-muted/30 p-4 rounded-lg">
              <div className="grid gap-4 lg:grid-cols-3">
                <SwitchSettingCard
                  title="启用代理池"
                  checked={proxyPool.enabled}
                  onCheckedChange={(checked) => updateProxyPool({ enabled: Boolean(checked) })}
                  ariaLabel="切换代理池"
                  description="未显式配置代理的新凭据会绑定到池内节点。"
                />
                <SwitchSettingCard
                  title="强制代理"
                  checked={proxyPool.requireProxy}
                  onCheckedChange={(checked) => updateProxyPool({ requireProxy: Boolean(checked) })}
                  ariaLabel="切换强制代理"
                  description="启用后，新凭据不能在没有可用代理节点时导入。"
                />
                <SwitchSettingCard
                  title="故障迁移"
                  checked={proxyPool.failover.enabled}
                  onCheckedChange={(checked) =>
                    updateProxyPoolFailover({ enabled: Boolean(checked) })
                  }
                  ariaLabel="切换代理故障迁移"
                  description="池内代理连续传输失败后自动迁移凭据绑定。"
                />
              </div>

              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <div className="space-y-2">
                  <label className="text-sm font-medium" htmlFor="proxyAssignmentStrategy">
                    分配策略
                  </label>
                  <select
                    id="proxyAssignmentStrategy"
                    value={proxyPool.assignmentStrategy}
                    onChange={(e) =>
                      updateProxyPool({
                        assignmentStrategy: e.target.value as ProxyPoolConfig['assignmentStrategy'],
                      })
                    }
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                  >
                    <option value="weighted_least_assigned">权重均衡</option>
                    <option value="hash">固定哈希</option>
                  </select>
                </div>

                <div className="space-y-2">
                  <label className="text-sm font-medium" htmlFor="proxyFailureThreshold">
                    故障阈值
                  </label>
                  <Input
                    id="proxyFailureThreshold"
                    type="number"
                    min="1"
                    step="1"
                    value={proxyPool.failover.failureThreshold}
                    onChange={(e) =>
                      updateProxyPoolFailover({
                        failureThreshold: Number.parseInt(e.target.value || '0', 10),
                      })
                    }
                  />
                </div>

                <div className="space-y-2">
                  <label className="text-sm font-medium" htmlFor="proxyCooldownSecs">
                    冷却秒数
                  </label>
                  <Input
                    id="proxyCooldownSecs"
                    type="number"
                    min="1"
                    step="1"
                    value={proxyPool.failover.cooldownSecs}
                    onChange={(e) =>
                      updateProxyPoolFailover({
                        cooldownSecs: Number.parseInt(e.target.value || '0', 10),
                      })
                    }
                  />
                </div>

                <div className="space-y-2">
                  <label className="text-sm font-medium" htmlFor="proxyProbeUrl">
                    探测 URL
                  </label>
                  <Input
                    id="proxyProbeUrl"
                    placeholder="可选"
                    value={proxyPool.failover.probeUrl ?? ''}
                    onChange={(e) =>
                      updateProxyPoolFailover({ probeUrl: e.target.value || null })
                    }
                  />
                </div>
              </div>

              <div className="space-y-3">
                {proxyPool.proxies.length === 0 ? (
                  <div className="rounded-md border border-dashed bg-background/50 px-3 py-4 text-sm text-muted-foreground">
                    暂无代理节点
                  </div>
                ) : (
                  proxyPool.proxies.map((proxy, index) => (
                    <div
                      key={`${proxy.id}-${index}`}
                      className="grid gap-3 rounded-md border bg-background/50 p-3 lg:grid-cols-[minmax(120px,0.8fr)_minmax(220px,1.6fr)_80px_90px_minmax(140px,1fr)_minmax(140px,1fr)_minmax(120px,1fr)_44px]"
                    >
                      <div className="space-y-1.5">
                        <label className="text-xs font-medium text-muted-foreground">ID</label>
                        <Input
                          value={proxy.id}
                          onChange={(e) => updateProxyPoolEntry(index, { id: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1.5">
                        <label className="text-xs font-medium text-muted-foreground">URL</label>
                        <Input
                          value={proxy.url}
                          placeholder="http://host:port 或 socks5://host:port"
                          onChange={(e) => updateProxyPoolEntry(index, { url: e.target.value })}
                        />
                      </div>
                      <div className="space-y-1.5">
                        <label className="text-xs font-medium text-muted-foreground">权重</label>
                        <Input
                          type="number"
                          min="1"
                          step="1"
                          value={proxy.weight}
                          onChange={(e) =>
                            updateProxyPoolEntry(index, {
                              weight: Number.parseInt(e.target.value || '0', 10),
                            })
                          }
                        />
                      </div>
                      <div className="space-y-1.5">
                        <label className="text-xs font-medium text-muted-foreground">启用</label>
                        <div className="flex h-10 items-center">
                          <Switch
                            checked={proxy.enabled}
                            onCheckedChange={(checked) =>
                              updateProxyPoolEntry(index, { enabled: Boolean(checked) })
                            }
                          />
                        </div>
                      </div>
                      <div className="space-y-1.5">
                        <label className="text-xs font-medium text-muted-foreground">用户名</label>
                        <Input
                          value={proxy.username ?? ''}
                          onChange={(e) =>
                            updateProxyPoolEntry(index, { username: e.target.value || null })
                          }
                        />
                      </div>
                      <div className="space-y-1.5">
                        <label className="text-xs font-medium text-muted-foreground">密码</label>
                        <Input
                          type="password"
                          value={proxy.password ?? ''}
                          onChange={(e) =>
                            updateProxyPoolEntry(index, { password: e.target.value || null })
                          }
                        />
                      </div>
                      <div className="space-y-1.5">
                        <label className="text-xs font-medium text-muted-foreground">出口 IP</label>
                        <Input
                          value={proxy.expectedEgressIp ?? ''}
                          onChange={(e) =>
                            updateProxyPoolEntry(index, {
                              expectedEgressIp: e.target.value || null,
                            })
                          }
                        />
                      </div>
                      <div className="flex items-end">
                        <Button
                          type="button"
                          variant="outline"
                          size="icon"
                          onClick={() => removeProxyPoolEntry(index)}
                          aria-label="删除代理节点"
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>

          <div className="space-y-4">
            <h3 className="text-sm font-semibold flex items-center text-primary">队列控制配置</h3>
            <div className="grid gap-4 bg-muted/30 p-4 rounded-lg md:grid-cols-2">
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
            <h3 className="text-sm font-semibold flex items-center text-primary">限流与恢复策略</h3>
            <div className="space-y-4 bg-muted/30 p-4 rounded-lg">
              <div className="grid gap-4 lg:grid-cols-2 2xl:grid-cols-3">
                <SwitchSettingCard
                  title="429 冷却与退避"
                  checked={rateLimitCooldownEnabled}
                  onCheckedChange={setRateLimitCooldownEnabled}
                  ariaLabel="切换 429 冷却与退避"
                  description={
                    <>
                      <p>控制上游 429 后是否写入账号级冷却，并同步下调 token bucket 的当前回填速率。</p>
                      <p>默认关闭。关闭后遇到 429 只保留请求级重试。</p>
                    </>
                  }
                />

                <SwitchSettingCard
                  title="Suspicious activity 长冷却"
                  checked={suspiciousActivityCooldownEnabled}
                  onCheckedChange={setSuspiciousActivityCooldownEnabled}
                  ariaLabel="切换 suspicious activity 长冷却"
                  description={
                    <>
                      <p>命中 Kiro 风控临时限制后，对该凭据写入独立的账号级长冷却。</p>
                      <p>默认启用，且独立于普通 429 冷却开关；冷却时间设为 0 时只做 bucket 退避。</p>
                    </>
                  }
                />

                <SwitchSettingCard
                  title="优先干净账号"
                  checked={suspiciousActivityPreferCleanCredentials}
                  onCheckedChange={setSuspiciousActivityPreferCleanCredentials}
                  ariaLabel="切换优先干净账号"
                  description="调度时优先选择从未命中过 suspicious activity 的凭据；历史命中过的账号仅作为兜底候选。"
                />

                <SwitchSettingCard
                  title="Suspicious activity 自动停调"
                  checked={suspiciousActivityAutoDisableEnabled}
                  onCheckedChange={setSuspiciousActivityAutoDisableEnabled}
                  ariaLabel="切换 suspicious activity 自动停调"
                  description="同一账号在统计窗口内多次命中 suspicious activity 后自动禁用，避免继续探测上游风控。"
                />

                <SwitchSettingCard
                  title="Suspicious activity 自动恢复"
                  checked={suspiciousActivityAutoClearEnabled}
                  onCheckedChange={setSuspiciousActivityAutoClearEnabled}
                  ariaLabel="切换 suspicious activity 自动恢复"
                  description="隔离结束后，账号连续成功或长期未再命中 suspicious activity 时自动清除历史标记。"
                />

                <SwitchSettingCard
                  title="模型冷却"
                  checked={modelCooldownEnabled}
                  onCheckedChange={setModelCooldownEnabled}
                  ariaLabel="切换模型冷却"
                  description="遇到上游 `INVALID_MODEL_ID` 时，是否把该模型族加入账号级运行时临时限制。默认开启。"
                  warning="关闭后会清空已有运行时模型限制。"
                />

                <SwitchSettingCard
                  title="会话凭据亲和"
                  checked={sessionAffinityEnabled}
                  onCheckedChange={setSessionAffinityEnabled}
                  ariaLabel="切换会话凭据亲和"
                  description={
                    <>
                      <p>同一 Claude 会话优先复用上次成功的 Kiro 凭据；凭据不可调度时自动回退现有策略。</p>
                      <p>默认关闭。启用后按模型和会话维度缓存 1 小时，多副本优先使用 Redis 共享缓存。</p>
                    </>
                  }
                />
              </div>

              <div className="space-y-3 rounded-lg border bg-background/50 p-4">
                <div className="space-y-1">
                  <div className="text-sm font-medium">数值参数</div>
                  <p className="text-sm text-muted-foreground">
                    集中管理冷却时长、统计窗口、自动恢复阈值和默认并发上限。
                  </p>
                </div>
                <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
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
                    <label className="text-sm font-medium" htmlFor="suspiciousActivityCooldownMs">
                      Suspicious activity 全局冷却时间 (毫秒)
                    </label>
                    <Input
                      id="suspiciousActivityCooldownMs"
                      type="number"
                      min="0"
                      step="60000"
                      value={suspiciousActivityCooldownMsInput}
                      onChange={(e) => setSuspiciousActivityCooldownMsInput(e.target.value)}
                      placeholder="默认 7200000，即 2 小时"
                    />
                  </div>

                  <div className="space-y-2">
                    <label className="text-sm font-medium" htmlFor="suspiciousActivityAutoDisableThreshold">
                      自动停调阈值
                    </label>
                    <Input
                      id="suspiciousActivityAutoDisableThreshold"
                      type="number"
                      min="0"
                      step="1"
                      value={suspiciousActivityAutoDisableThresholdInput}
                      onChange={(e) => setSuspiciousActivityAutoDisableThresholdInput(e.target.value)}
                      placeholder="默认 3"
                    />
                  </div>

                  <div className="space-y-2">
                    <label className="text-sm font-medium" htmlFor="suspiciousActivityAutoDisableWindowMs">
                      自动停调统计窗口 (毫秒)
                    </label>
                    <Input
                      id="suspiciousActivityAutoDisableWindowMs"
                      type="number"
                      min="0"
                      step="60000"
                      value={suspiciousActivityAutoDisableWindowMsInput}
                      onChange={(e) => setSuspiciousActivityAutoDisableWindowMsInput(e.target.value)}
                      placeholder="默认 86400000，即 24 小时"
                    />
                  </div>

                  <div className="space-y-2">
                    <label className="text-sm font-medium" htmlFor="suspiciousActivityAutoClearSuccessThreshold">
                      自动恢复成功次数
                    </label>
                    <Input
                      id="suspiciousActivityAutoClearSuccessThreshold"
                      type="number"
                      min="0"
                      step="1"
                      value={suspiciousActivityAutoClearSuccessThresholdInput}
                      onChange={(e) => setSuspiciousActivityAutoClearSuccessThresholdInput(e.target.value)}
                      placeholder="默认 10"
                    />
                  </div>

                  <div className="space-y-2">
                    <label className="text-sm font-medium" htmlFor="suspiciousActivityAutoClearAfterMs">
                      自动恢复未命中时间 (毫秒)
                    </label>
                    <Input
                      id="suspiciousActivityAutoClearAfterMs"
                      type="number"
                      min="0"
                      step="60000"
                      value={suspiciousActivityAutoClearAfterMsInput}
                      onChange={(e) => setSuspiciousActivityAutoClearAfterMsInput(e.target.value)}
                      placeholder="默认 604800000，即 7 天"
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
            </div>
          </div>

          <div className="space-y-4">
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

          <div className="space-y-4">
            <h3 className="text-sm font-semibold flex items-center text-primary">轻 / 重请求加权</h3>
            <div className="space-y-4 rounded-lg bg-muted/30 p-4">
              <SwitchSettingCard
                title="按请求复杂度动态消耗 bucket"
                checked={requestWeightingEnabled}
                onCheckedChange={setRequestWeightingEnabled}
                ariaLabel="切换轻重请求加权"
                disabledLabel="已禁用"
                description="适配轻请求和重代码请求混跑。启用后，tools、thinking、大输入和高 maxTokens 请求会消耗更多本地 bucket 配额。"
              />

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

          <div className="space-y-4">
            <h3 className="text-sm font-semibold flex items-center text-primary">流式响应头故障转移</h3>
            <div className="space-y-4 rounded-lg bg-muted/30 p-4">
              <SwitchSettingCard
                title="启用自适应 pre-SSE 快速切换"
                checked={streamPreSseFailoverEnabled}
                onCheckedChange={setStreamPreSseFailoverEnabled}
                ariaLabel="切换自适应 pre-SSE 快速切换"
                disabledLabel="仅使用总预算"
                description={
                  <>
                    <p>流式请求在上游长期不返回响应头时，按请求大小和模型给单个凭据设置等待上限。</p>
                    <p>超过快速切换次数后，最后一次尝试使用剩余总预算，避免大请求被频繁打断。</p>
                  </>
                }
              />

              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                {STREAM_PRE_SSE_FAILOVER_FIELDS.map((field) => (
                  <div key={field.key} className="space-y-2">
                    <label className="text-sm font-medium" htmlFor={`streamPreSse-${field.key}`}>
                      {field.label}
                    </label>
                    <Input
                      id={`streamPreSse-${field.key}`}
                      type="number"
                      min={field.min}
                      step={field.step}
                      value={streamPreSseFailoverInputs[field.key]}
                      onChange={(e) => handleStreamPreSseFailoverInputChange(field.key, e.target.value)}
                      placeholder={field.placeholder}
                    />
                    <p className="text-xs text-muted-foreground">{field.hint}</p>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="space-y-4">
            <h3 className="text-sm font-semibold flex items-center text-primary">非流式响应体超时</h3>
            <div className="space-y-4 rounded-lg bg-muted/30 p-4">
              <div className="grid gap-4 lg:grid-cols-3">
                <SwitchSettingCard
                  title="限制完整 body 读取时间"
                  checked={nonStreamBodyReadTimeoutEnabled}
                  onCheckedChange={setNonStreamBodyReadTimeoutEnabled}
                  ariaLabel="切换非流式响应体读取超时"
                  disabledLabel="不限制"
                  description={
                    <>
                      <p>非流式请求收到上游响应头后，对读取完整响应体设置独立总预算。</p>
                      <p>默认启用，避免上游已返回响应头但 body 长时间不结束时占满 AgentGear 总超时。</p>
                    </>
                  }
                />

                <SwitchSettingCard
                  title="EventStream 卡住时安全重试"
                  checked={nonStreamEventstreamSafeRetryOnStall}
                  onCheckedChange={setNonStreamEventstreamSafeRetryOnStall}
                  ariaLabel="切换非流式 EventStream 卡住安全重试"
                  disabledLabel="不额外重试"
                  description={
                    <>
                      <p>上游已返回 EventStream 且尚未产生可用输出时，空闲超时后允许一次切换凭据。</p>
                      <p>已收到 assistant 文本、工具调用、错误或解码异常时不会触发，避免重复执行。</p>
                    </>
                  }
                />

                <SwitchSettingCard
                  title="超时后切换凭据"
                  checked={nonStreamBodyReadTimeoutRetryOnTimeout}
                  onCheckedChange={setNonStreamBodyReadTimeoutRetryOnTimeout}
                  ariaLabel="切换非流式 body 超时后重试"
                  disabledLabel="直接返回 504"
                  description={
                    <>
                      <p>body 读取超时后把当前凭据视为本请求瞬态失败，并尝试其他候选凭据。</p>
                      <p>默认关闭，避免大请求在多个凭据上重复等待而拖长总耗时。</p>
                    </>
                  }
                />
              </div>

              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                <div className="space-y-2">
                  <label className="text-sm font-medium" htmlFor="nonStreamBodyReadTimeoutMs">
                    完整 body 读取超时 (毫秒)
                  </label>
                  <Input
                    id="nonStreamBodyReadTimeoutMs"
                    type="number"
                    min="0"
                    step="1000"
                    value={nonStreamBodyReadTimeoutMsInput}
                    onChange={(e) => setNonStreamBodyReadTimeoutMsInput(e.target.value)}
                    placeholder="默认 540000"
                  />
                  <p className="text-xs text-muted-foreground">
                    建议低于 AgentGear 总超时，便于 kiro-rs 先返回明确 504 诊断。
                  </p>
                </div>
                <div className="space-y-2">
                  <label className="text-sm font-medium" htmlFor="nonStreamEventstreamIdleTimeoutMs">
                    EventStream 空闲超时 (毫秒)
                  </label>
                  <Input
                    id="nonStreamEventstreamIdleTimeoutMs"
                    type="number"
                    min="0"
                    step="1000"
                    value={nonStreamEventstreamIdleTimeoutMsInput}
                    onChange={(e) => setNonStreamEventstreamIdleTimeoutMsInput(e.target.value)}
                    placeholder="默认 120000"
                  />
                  <p className="text-xs text-muted-foreground">
                    仅在非流式上游返回 Amazon EventStream 且 body 已开始后生效。
                  </p>
                </div>
              </div>
            </div>
          </div>

          <div className="space-y-4">
            <h3 className="text-sm font-semibold flex items-center text-primary">Kiro 上游请求体保护</h3>
            <div className="space-y-4 rounded-lg bg-muted/30 p-4">
              <div className="grid gap-4 lg:grid-cols-3">
                <SwitchSettingCard
                  title="最终 body 大小拦截"
                  checked={kiroRequestBodyGuardEnabled}
                  onCheckedChange={setKiroRequestBodyGuardEnabled}
                  ariaLabel="切换 Kiro 上游请求体大小拦截"
                  disabledLabel="不拦截"
                  description={
                    <>
                      <p>在 profileArn 注入后、发往 Kiro 上游前检查最终 JSON body 长度。</p>
                      <p>命中后返回 context_length_exceeded，让客户端按上下文超限路径压缩。</p>
                    </>
                  }
                />

                <div className="space-y-2 rounded-lg border bg-background/70 p-4 lg:col-span-2">
                  <label className="text-sm font-medium" htmlFor="kiroRequestBodyGuardMaxMiB">
                    上游 body 上限 (MiB)
                  </label>
                  <Input
                    id="kiroRequestBodyGuardMaxMiB"
                    type="number"
                    min="1"
                    max="64"
                    step="0.5"
                    value={kiroRequestBodyGuardMaxMiBInput}
                    onChange={(e) => setKiroRequestBodyGuardMaxMiBInput(e.target.value)}
                    placeholder="默认 30"
                  />
                  <p className="text-xs text-muted-foreground">
                    默认 30MiB；保存时按 MiB 转换为字节写入运行时配置。
                  </p>
                </div>
              </div>
            </div>
          </div>

          <div className="space-y-4">
            <h3 className="text-sm font-semibold flex items-center text-primary">Thinking 签名校验</h3>
            <div className="space-y-4 rounded-lg bg-muted/30 p-4">
              <div className="flex flex-col gap-3 rounded-lg border bg-background/70 p-4 md:flex-row md:items-center md:justify-between">
                <div className="space-y-1">
                  <div className="text-sm font-medium">历史 thinking signature 处理策略</div>
                  <p className="text-sm text-muted-foreground">
                    控制下游请求携带的历史 thinking signature 校验失败时，是拒绝、告警放行、剥离后放行，还是跳过校验。
                  </p>
                </div>
                <Badge variant={thinkingSignatureValidationMode === 'strict' ? 'secondary' : 'outline'}>
                  {THINKING_SIGNATURE_VALIDATION_OPTIONS.find((option) => option.value === thinkingSignatureValidationMode)?.label}
                </Badge>
              </div>

              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                {THINKING_SIGNATURE_VALIDATION_OPTIONS.map((option) => (
                  <Button
                    key={option.value}
                    type="button"
                    variant={thinkingSignatureValidationMode === option.value ? 'default' : 'outline'}
                    className="h-auto min-h-[108px] flex-col items-start justify-start whitespace-normal px-4 py-3 text-left"
                    onClick={() => setThinkingSignatureValidationMode(option.value)}
                  >
                    <span className="text-sm font-semibold">{option.label}</span>
                    <span className="text-xs font-normal leading-relaxed opacity-80">
                      {option.description}
                    </span>
                  </Button>
                ))}
              </div>

              <div className="flex flex-col gap-3 rounded-lg border bg-background/70 p-4 md:flex-row md:items-center md:justify-between">
                <div className="space-y-1">
                  <div className="text-sm font-medium">流式响应首内容后释放调度 lease</div>
                  <p className="text-sm text-muted-foreground">
                    上游流已开始产生可转发内容后释放；无法探测首内容时，在响应建立后释放。
                  </p>
                </div>
                <Switch
                  checked={streamDispatchLeaseReleaseEnabled}
                  onCheckedChange={setStreamDispatchLeaseReleaseEnabled}
                />
              </div>

              <div className="flex flex-col gap-3 rounded-lg border bg-background/70 p-4 md:flex-row md:items-center md:justify-between">
                <div className="space-y-1">
                  <div className="text-sm font-medium">响应侧隐藏 thinking signature 补齐</div>
                  <p className="text-sm text-muted-foreground">
                    thinking 流式请求先返回普通内容时，补齐隐藏 thinking block 和动态 AWS-shaped signature_delta。
                  </p>
                </div>
                <Switch
                  checked={responseThinkingSignatureCompatEnabled}
                  onCheckedChange={setResponseThinkingSignatureCompatEnabled}
                />
              </div>
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
      </div>
    </TooltipProvider>
  )
}
