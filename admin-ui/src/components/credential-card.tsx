import { useState, type ComponentType } from 'react'
import { toast } from 'sonner'
import {
  BadgeCheck,
  Building2,
  ChevronDown,
  ChevronUp,
  Clock3,
  Eraser,
  Globe2,
  KeyRound,
  Layers,
  Link2,
  Loader2,
  MoreVertical,
  Network,
  RefreshCw,
  Server,
  ShieldCheck,
  Shuffle,
  Tags,
  Trash2,
  UserRound,
  Wallet,
  Zap,
} from 'lucide-react'
import {
  AccountTypeInput,
  findStandardAccountTypePreset,
  ModelSelector,
} from '@/components/model-policy-controls'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { Input } from '@/components/ui/input'
import { Checkbox } from '@/components/ui/checkbox'
import { Progress } from '@/components/ui/progress'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import type {
  CredentialStatusItem,
  BalanceResponse,
  AvailableProfile,
  CredentialProxyMode,
  ModelCatalogItem,
  ProxyPoolEntry,
  StandardAccountTypePreset,
} from '@/types/api'
import {
  useSetDisabled,
  useClearCredentialRuntimeModelRestrictions,
  useClearCredentialSuspiciousActivity,
  useSetCredentialModelPolicy,
  useSetCredentialSource,
  useSetCredentialGroups,
  useSetCredentialProxy,
  useCredentialProfiles,
  useSetCredentialProfile,
  useSetCredentialRateLimitConfig,
  useSetMaxConcurrency,
  useSetPriority,
  useResetFailure,
  useDeleteCredential,
  useForceRefreshToken,
  useLoadBalancingMode,
} from '@/hooks/use-credentials'
import { CredentialGroupPicker } from '@/components/credential-group-picker'
import { getCredentialLabel, getCredentialLabelWithId } from '@/lib/credential-label'
import { formatCredentialGroupsInput, normalizeCredentialGroups } from '@/lib/credential-groups'
import { cn } from '@/lib/utils'

interface CredentialCardProps {
  credential: CredentialStatusItem
  onViewBalance: (id: number) => void
  selected: boolean
  onToggleSelect: () => void
  balance: BalanceResponse | null
  loadingBalance: boolean
  accountTypeSuggestions: string[]
  standardAccountTypePresets: StandardAccountTypePreset[]
  modelCatalog: ModelCatalogItem[]
}

type RateLimitCooldownMode = 'global' | 'enabled' | 'disabled'

const compactNumberFormatter = new Intl.NumberFormat('zh-CN', {
  notation: 'compact',
  maximumFractionDigits: 1,
})
const integerNumberFormatter = new Intl.NumberFormat('zh-CN')

function formatTokenCount(value: number | null | undefined): string {
  const normalized = Number.isFinite(value ?? 0) ? Math.max(0, value ?? 0) : 0
  return compactNumberFormatter.format(normalized)
}

function formatInteger(value: number | null | undefined): string {
  const normalized = Number.isFinite(value ?? 0) ? Math.max(0, value ?? 0) : 0
  return integerNumberFormatter.format(normalized)
}

function rateLimitCooldownModeFromOverride(value: boolean | null | undefined): RateLimitCooldownMode {
  if (value === true) return 'enabled'
  if (value === false) return 'disabled'
  return 'global'
}

function rateLimitCooldownOverrideFromMode(mode: RateLimitCooldownMode): boolean | null {
  if (mode === 'enabled') return true
  if (mode === 'disabled') return false
  return null
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

function splitEmailLabel(value: string): { localPart: string; domain: string } | null {
  const atIndex = value.lastIndexOf('@')
  if (atIndex <= 0 || atIndex >= value.length - 1) {
    return null
  }

  return {
    localPart: value.slice(0, atIndex),
    domain: value.slice(atIndex + 1),
  }
}

function CompactPill({
  icon: Icon,
  label,
  title,
  tone = 'muted',
}: {
  icon?: ComponentType<{ className?: string }>
  label: string
  title?: string
  tone?: 'muted' | 'accent' | 'success'
}) {
  return (
    <span
      className={cn(
        'inline-flex h-5 max-w-full items-center gap-1 rounded-full border px-1.5 text-[11px] font-medium leading-none',
        tone === 'accent' && 'border-primary/20 bg-primary/10 text-primary',
        tone === 'success' && 'border-green-500/20 bg-green-500/10 text-green-700',
        tone === 'muted' && 'border-border bg-background text-muted-foreground'
      )}
      title={title ?? label}
    >
      {Icon && <Icon className="h-3 w-3 shrink-0" />}
      <span className="min-w-0 truncate">{label}</span>
    </span>
  )
}

function formatDispatchSourceLabel(
  source:
    | CredentialStatusItem['maxConcurrencySource']
    | CredentialStatusItem['rateLimitBucketCapacitySource']
    | CredentialStatusItem['rateLimitCooldownEnabledSource']
): string | null {
  switch (source) {
    case 'credential':
      return '凭据显式覆盖'
    case 'account-type':
      return '账号类型策略'
    case 'global-default':
      return '全局默认'
    default:
      return null
  }
}

function formatAccountTypeSourceLabel(
  source: CredentialStatusItem['accountTypeSource']
): string | null {
  switch (source) {
    case 'credential':
      return '显式账号类型'
    case 'subscription-title':
      return '按订阅档位推断'
    case 'subscription-type':
      return '按订阅类型推断'
    default:
      return null
  }
}

function formatAccountTypeSourceShortLabel(
  source: CredentialStatusItem['accountTypeSource']
): string | null {
  switch (source) {
    case 'credential':
      return '显式'
    case 'subscription-title':
      return '档位'
    case 'subscription-type':
      return '类型'
    default:
      return null
  }
}

function formatAuthAccountTypeLabel(value: CredentialStatusItem['authAccountType']): string {
  switch (value) {
    case 'social':
      return 'Social'
    case 'builder-id':
      return 'Builder ID'
    case 'enterprise':
      return 'Enterprise'
    case 'idc':
      return 'IdC'
    default:
      return value || '未知'
  }
}

function formatAccountTypeCompactLabel(value: string | null | undefined): string {
  switch (value) {
    case 'pro-plus':
      return 'PRO+'
    case 'builder-id':
      return 'Builder'
    case 'idc':
      return 'IdC'
    default:
      return value || '未设置'
  }
}

function formatSubscriptionTypeCompactLabel(value: string | null | undefined): string | null {
  if (!value) return null

  const normalized = value.toUpperCase()
  if (normalized.includes('PRO_PLUS')) return 'PRO+'
  if (normalized.includes('ULTRA')) return 'ULTRA'
  if (normalized.includes('MAX')) return 'MAX'
  if (normalized.includes('POWER')) return 'POWER'
  if (normalized.includes('FREE')) return 'FREE'
  if (normalized.includes('PRO')) return 'PRO'

  return value.length > 16 ? `${value.slice(0, 8)}…${value.slice(-5)}` : value
}

function formatProfileArnCompact(value: string | null | undefined): string | null {
  const trimmed = value?.trim()
  if (!trimmed) return null

  const resourceName = trimmed.split('/').pop()
  const label = resourceName && resourceName !== trimmed ? resourceName : trimmed

  return label.length > 22 ? `${label.slice(0, 10)}…${label.slice(-8)}` : label
}

function formatProfileOptionLabel(profile: AvailableProfile): string {
  const name = profile.profileName?.trim()
  const type = profile.profileType?.trim()
  const arnLabel = formatProfileArnCompact(profile.arn) ?? profile.arn

  if (name && type) {
    return `${name} (${type}) - ${arnLabel}`
  }
  if (name) {
    return `${name} - ${arnLabel}`
  }
  if (type) {
    return `${type} - ${arnLabel}`
  }
  return arnLabel
}

function getAuthAccountTypeIcon(
  value: CredentialStatusItem['authAccountType']
): ComponentType<{ className?: string }> {
  switch (value) {
    case 'social':
      return UserRound
    case 'builder-id':
      return BadgeCheck
    case 'enterprise':
      return Building2
    case 'idc':
      return KeyRound
    default:
      return KeyRound
  }
}

function initialProxyMode(credential: CredentialStatusItem): CredentialProxyMode {
  const proxyUrl = credential.proxyUrl?.trim()
  if (proxyUrl) {
    return proxyUrl.toLowerCase() === 'direct' ? 'direct' : 'custom'
  }
  if (credential.proxyId?.trim()) {
    return 'pool'
  }
  return 'global'
}

function proxyPoolEntryLabel(proxy: ProxyPoolEntry): string {
  const egress = proxy.expectedEgressIp ? ` (${proxy.expectedEgressIp})` : ''
  const assigned = typeof proxy.assignedCredentials === 'number'
    ? ` · 已挂载 ${proxy.assignedCredentials} 凭据`
    : ''
  return `${proxy.id}${egress}${assigned}`
}

function credentialProxySummary(
  credential: CredentialStatusItem,
  proxyPoolEntries: ProxyPoolEntry[]
): { label: string; detail: string; tone: 'muted' | 'accent' | 'success' } {
  const proxyUrl = credential.proxyUrl?.trim()
  if (proxyUrl) {
    if (proxyUrl.toLowerCase() === 'direct') {
      return { label: '直连', detail: '显式绕过全局代理和代理池', tone: 'muted' }
    }
    return { label: '自定义代理', detail: proxyUrl, tone: 'accent' }
  }

  const proxyId = credential.proxyId?.trim()
  if (proxyId) {
    const entry = proxyPoolEntries.find((proxy) => proxy.id === proxyId)
    return {
      label: entry ? `代理池 ${proxyPoolEntryLabel(entry)}` : `代理池 ${proxyId}`,
      detail: entry?.url ?? `proxyId: ${proxyId}`,
      tone: 'success',
    }
  }

  return { label: '全局/未绑定', detail: '未设置凭据级代理绑定', tone: 'muted' }
}

export function CredentialCard({
  credential,
  onViewBalance,
  selected,
  onToggleSelect,
  balance,
  loadingBalance,
  accountTypeSuggestions,
  standardAccountTypePresets,
  modelCatalog,
}: CredentialCardProps) {
  const [priorityValue, setPriorityValue] = useState(String(credential.priority))
  const [maxConcurrencyValue, setMaxConcurrencyValue] = useState(
    credential.maxConcurrencyOverride ? String(credential.maxConcurrencyOverride) : ''
  )
  const [rateLimitCooldownMode, setRateLimitCooldownMode] = useState<RateLimitCooldownMode>(
    rateLimitCooldownModeFromOverride(credential.rateLimitCooldownEnabledOverride)
  )
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
  const [sourceSupplierNameValue, setSourceSupplierNameValue] = useState(
    credential.sourceSupplierName ?? ''
  )
  const [sourceSupplierIdValue, setSourceSupplierIdValue] = useState(
    credential.sourceSupplierId ?? ''
  )
  const [sourceBatchValue, setSourceBatchValue] = useState(credential.sourceBatch ?? '')
  const [credentialGroupsValue, setCredentialGroupsValue] = useState(
    formatCredentialGroupsInput(credential.credentialGroups ?? [])
  )
  const [allowedModelsValue, setAllowedModelsValue] = useState(credential.allowedModels ?? [])
  const [blockedModelsValue, setBlockedModelsValue] = useState(credential.blockedModels ?? [])
  const [clearRuntimeModelRestrictions, setClearRuntimeModelRestrictions] = useState(false)
  const [selectedProfileArn, setSelectedProfileArn] = useState(credential.profileArn ?? '')
  const [profileSelectionTouched, setProfileSelectionTouched] = useState(false)
  const [showDeleteDialog, setShowDeleteDialog] = useState(false)
  const [showProxyDialog, setShowProxyDialog] = useState(false)
  const [proxyMode, setProxyMode] = useState<CredentialProxyMode>(initialProxyMode(credential))
  const [proxyIdValue, setProxyIdValue] = useState(credential.proxyId ?? '')
  const [proxyUrlValue, setProxyUrlValue] = useState(credential.proxyUrl ?? '')
  const [proxyUsernameValue, setProxyUsernameValue] = useState('')
  const [proxyPasswordValue, setProxyPasswordValue] = useState('')

  const setDisabled = useSetDisabled()
  const clearRuntimeModelCooldown = useClearCredentialRuntimeModelRestrictions()
  const clearSuspiciousActivity = useClearCredentialSuspiciousActivity()
  const setModelPolicy = useSetCredentialModelPolicy()
  const setSource = useSetCredentialSource()
  const setGroups = useSetCredentialGroups()
  const setProxy = useSetCredentialProxy()
  const profileQuery = useCredentialProfiles(credential.id, showModelPolicyDialog)
  const setProfile = useSetCredentialProfile()
  const setMaxConcurrency = useSetMaxConcurrency()
  const setRateLimitConfig = useSetCredentialRateLimitConfig()
  const setPriority = useSetPriority()
  const resetFailure = useResetFailure()
  const deleteCredential = useDeleteCredential()
  const forceRefresh = useForceRefreshToken()
  const { data: loadBalancingData } = useLoadBalancingMode()

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

  const handleClearRuntimeModelRestrictions = () => {
    clearRuntimeModelCooldown.mutate(credential.id, {
      onSuccess: (res) => {
        toast.success(res.message)
      },
      onError: (err) => {
        toast.error('清除失败: ' + (err as Error).message)
      },
    })
  }

  const handleClearSuspiciousActivity = () => {
    clearSuspiciousActivity.mutate(credential.id, {
      onSuccess: (res) => {
        toast.success(res.message)
      },
      onError: (err) => {
        toast.error('清除失败: ' + (err as Error).message)
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
    setPriorityValue(String(credential.priority))
    setMaxConcurrencyValue(
      credential.maxConcurrencyOverride !== undefined && credential.maxConcurrencyOverride !== null
        ? String(credential.maxConcurrencyOverride)
        : ''
    )
    setRateLimitCooldownMode(
      rateLimitCooldownModeFromOverride(credential.rateLimitCooldownEnabledOverride)
    )
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
    setAccountTypeValue(credential.accountType ?? '')
    setSourceSupplierNameValue(credential.sourceSupplierName ?? '')
    setSourceSupplierIdValue(credential.sourceSupplierId ?? '')
    setSourceBatchValue(credential.sourceBatch ?? '')
    setCredentialGroupsValue(formatCredentialGroupsInput(credential.credentialGroups ?? []))
    setAllowedModelsValue(credential.allowedModels ?? [])
    setBlockedModelsValue(credential.blockedModels ?? [])
    setClearRuntimeModelRestrictions(false)
    setSelectedProfileArn(credential.profileArn?.trim() ?? '')
    setProfileSelectionTouched(false)
    setShowModelPolicyDialog(true)
  }

  const openProxyDialog = () => {
    const mode = initialProxyMode(credential)
    setProxyMode(mode)
    setProxyIdValue(credential.proxyId ?? '')
    setProxyUrlValue(mode === 'custom' ? credential.proxyUrl ?? '' : '')
    setProxyUsernameValue('')
    setProxyPasswordValue('')
    setShowProxyDialog(true)
  }

  const allowedModelsSummary = summarizeSelectedModels(allowedModelsValue, modelCatalog)
  const blockedModelsSummary = summarizeSelectedModels(blockedModelsValue, modelCatalog)
  const savedProfileArn = credential.profileArn?.trim() ?? ''
  const profileOptions = (profileQuery.data?.profiles ?? []).filter(
    (profile) => profile.arn.trim().length > 0
  )
  const profileSelectOptions =
    savedProfileArn && !profileOptions.some((profile) => profile.arn.trim() === savedProfileArn)
      ? [
          {
            arn: savedProfileArn,
            profileName: '当前保存',
            profileType: null,
          },
          ...profileOptions,
        ]
      : profileOptions
  const selectedProfileValue =
    selectedProfileArn.trim() ||
    profileQuery.data?.selectedProfileArn?.trim() ||
    savedProfileArn ||
    ''
  const compactProfileLabel = formatProfileArnCompact(savedProfileArn)
  const proxyPoolOptions =
    loadBalancingData?.proxyPool?.proxies.filter((proxy) => proxy.enabled) ?? []
  const proxyPoolEnabled = loadBalancingData?.proxyPool?.enabled ?? false
  const proxyRequireProxy = loadBalancingData?.proxyPool?.requireProxy ?? false
  const proxySummary = credentialProxySummary(credential, loadBalancingData?.proxyPool?.proxies ?? [])

  const isSaving =
    setPriority.isPending ||
    setMaxConcurrency.isPending ||
    setRateLimitConfig.isPending ||
    setProfile.isPending ||
    setModelPolicy.isPending ||
    setSource.isPending ||
    setGroups.isPending ||
    setProxy.isPending

  const handleProxySave = async () => {
    if (proxyMode === 'pool' && !proxyIdValue.trim()) {
      toast.error('请选择代理池节点')
      return
    }
    if (proxyMode === 'custom') {
      const url = proxyUrlValue.trim()
      if (!url) {
        toast.error('请输入代理 URL')
        return
      }
      if (url.toLowerCase() === 'direct') {
        toast.error('direct 请使用直连模式')
        return
      }
    }
    if ((proxyMode === 'direct' || proxyMode === 'global') && proxyRequireProxy) {
      toast.error('当前代理池要求每个凭据必须绑定代理')
      return
    }

    try {
      await setProxy.mutateAsync({
        id: credential.id,
        payload: {
          mode: proxyMode,
          proxyId: proxyMode === 'pool' ? proxyIdValue.trim() : undefined,
          proxyUrl: proxyMode === 'custom' ? proxyUrlValue.trim() : undefined,
          proxyUsername:
            proxyMode === 'custom' ? proxyUsernameValue.trim() || undefined : undefined,
          proxyPassword:
            proxyMode === 'custom' ? proxyPasswordValue.trim() || undefined : undefined,
        },
      })
      toast.success('代理配置已保存')
      setShowProxyDialog(false)
    } catch (err) {
      toast.error('保存代理配置失败: ' + (err as Error).message)
    }
  }

  const handleModelPolicySave = async () => {
    const newPriority = parseInt(priorityValue, 10)
    if (isNaN(newPriority) || newPriority < 0) {
      toast.error('优先级必须是非负整数')
      return
    }

    const trimmedConcurrency = maxConcurrencyValue.trim()
    const parsedConcurrency = trimmedConcurrency ? Number.parseInt(trimmedConcurrency, 10) : undefined
    if (
      trimmedConcurrency &&
      (parsedConcurrency === undefined || !Number.isInteger(parsedConcurrency) || parsedConcurrency <= 0)
    ) {
      toast.error('并发上限必须是大于 0 的整数，留空表示不限制')
      return
    }

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

    try {
      let changed = false

      if (newPriority !== credential.priority) {
        await setPriority.mutateAsync({ id: credential.id, priority: newPriority })
        changed = true
      }

      const currentConcurrencyOverride = credential.maxConcurrencyOverride ?? null
      const targetConcurrencyOverride = parsedConcurrency ?? null
      if (currentConcurrencyOverride !== targetConcurrencyOverride) {
        await setMaxConcurrency.mutateAsync({
          id: credential.id,
          maxConcurrency: targetConcurrencyOverride,
        })
        changed = true
      }

      const currentBucketOverride = credential.rateLimitBucketCapacityOverride ?? null
      const currentRefillOverride = credential.rateLimitRefillPerSecondOverride ?? null
      const currentRateLimitCooldownOverride = credential.rateLimitCooldownEnabledOverride ?? null
      const targetRateLimitCooldownOverride =
        rateLimitCooldownOverrideFromMode(rateLimitCooldownMode)
      const targetBucketOverride = parsedBucketCapacity ?? null
      const targetRefillOverride = parsedRefillPerSecond ?? null
      if (
        currentRateLimitCooldownOverride !== targetRateLimitCooldownOverride ||
        currentBucketOverride !== targetBucketOverride ||
        currentRefillOverride !== targetRefillOverride
      ) {
        await setRateLimitConfig.mutateAsync({
          id: credential.id,
          rateLimitCooldownEnabled: targetRateLimitCooldownOverride,
          rateLimitBucketCapacity: targetBucketOverride,
          rateLimitRefillPerSecond: targetRefillOverride,
        })
        changed = true
      }

      const currentProfileArn = credential.profileArn?.trim() ?? ''
      const targetProfileArn = selectedProfileValue.trim()
      if (profileSelectionTouched && currentProfileArn !== targetProfileArn) {
        if (!targetProfileArn) {
          toast.error('请选择 Profile')
          return
        }
        await setProfile.mutateAsync({
          id: credential.id,
          payload: { profileArn: targetProfileArn },
        })
        changed = true
      }

      const currentAccountType = credential.accountType ?? ''
      const targetAccountType = accountTypeValue.trim()
      const currentAllowed = credential.allowedModels ?? []
      const currentBlocked = credential.blockedModels ?? []
      const allowedChanged = JSON.stringify(currentAllowed.slice().sort()) !== JSON.stringify(allowedModelsValue.slice().sort())
      const blockedChanged = JSON.stringify(currentBlocked.slice().sort()) !== JSON.stringify(blockedModelsValue.slice().sort())

      if (
        currentAccountType !== targetAccountType ||
        allowedChanged ||
        blockedChanged ||
        clearRuntimeModelRestrictions
      ) {
        await setModelPolicy.mutateAsync({
          id: credential.id,
          payload: {
            accountType: targetAccountType ? targetAccountType : null,
            allowedModels: allowedModelsValue.length ? allowedModelsValue : null,
            blockedModels: blockedModelsValue.length ? blockedModelsValue : null,
            clearRuntimeModelRestrictions,
          },
        })
        changed = true
      }

      const currentSourceSupplierName = credential.sourceSupplierName?.trim() ?? ''
      const currentSourceSupplierId = credential.sourceSupplierId?.trim() ?? ''
      const currentSourceBatch = credential.sourceBatch?.trim() ?? ''
      const targetSourceSupplierName = sourceSupplierNameValue.trim()
      const targetSourceSupplierId = sourceSupplierIdValue.trim()
      const targetSourceBatch = sourceBatchValue.trim()

      if (
        currentSourceSupplierName !== targetSourceSupplierName ||
        currentSourceSupplierId !== targetSourceSupplierId ||
        currentSourceBatch !== targetSourceBatch
      ) {
        await setSource.mutateAsync({
          id: credential.id,
          payload: {
            sourceSupplierName: targetSourceSupplierName || null,
            sourceSupplierId: targetSourceSupplierId || null,
            sourceBatch: targetSourceBatch || null,
          },
        })
        changed = true
      }

      const currentCredentialGroups = normalizeCredentialGroups(credential.credentialGroups ?? [])
      const targetCredentialGroups = normalizeCredentialGroups(credentialGroupsValue)
      const groupsChanged =
        JSON.stringify(currentCredentialGroups) !== JSON.stringify(targetCredentialGroups)
      if (groupsChanged) {
        await setGroups.mutateAsync({
          id: credential.id,
          payload: { credentialGroups: targetCredentialGroups },
        })
        changed = true
      }

      if (changed) {
        toast.success('配置已保存')
      } else {
        toast.info('未做任何修改')
      }
      setShowModelPolicyDialog(false)
    } catch (err) {
      toast.error('保存失败: ' + (err as Error).message)
    }
  }

  const maxConcurrencySourceLabel = formatDispatchSourceLabel(credential.maxConcurrencySource)
  const maxConcurrencyPlaceholder =
    credential.maxConcurrency !== undefined && credential.maxConcurrency !== null
      ? `跟随 ${credential.maxConcurrency}`
      : '不限'
  const accountTypeSourceLabel = formatAccountTypeSourceLabel(credential.accountTypeSource)
  const accountTypeSourceShortLabel = formatAccountTypeSourceShortLabel(credential.accountTypeSource)
  const rateLimitOverrideSummary = [
    credential.rateLimitCooldownEnabledOverride === undefined ||
    credential.rateLimitCooldownEnabledOverride === null
      ? `429退避跟随${formatDispatchSourceLabel(credential.rateLimitCooldownEnabledSource) ?? '全局默认'}(${credential.rateLimitCooldownEnabled ? '开' : '关'})`
      : credential.rateLimitCooldownEnabled
        ? '429退避强制开启'
        : '429退避强制关闭',
    credential.rateLimitBucketCapacityOverride === undefined ||
    credential.rateLimitBucketCapacityOverride === null
      ? `Bucket 跟随${formatDispatchSourceLabel(credential.rateLimitBucketCapacitySource) ?? '全局默认'}`
      : credential.rateLimitBucketCapacityOverride === 0
        ? 'Bucket 已禁用'
        : `Bucket=${credential.rateLimitBucketCapacityOverride}`,
    credential.rateLimitRefillPerSecondOverride === undefined ||
    credential.rateLimitRefillPerSecondOverride === null
      ? `回填跟随${formatDispatchSourceLabel(credential.rateLimitRefillPerSecondSource) ?? '全局默认'}`
      : credential.rateLimitRefillPerSecondOverride === 0
        ? '回填已禁用'
        : `回填=${credential.rateLimitRefillPerSecondOverride} token/s`,
  ].join(' / ')
  const policySummary = `允许 ${credential.allowedModels?.length ?? 0} 项 / 禁用 ${credential.blockedModels?.length ?? 0} 项`
  const hasRuntimeRestrictions = (credential.runtimeModelRestrictions?.length ?? 0) > 0
  const balanceSummary = loadingBalance
    ? null
    : balance
      ? `${balance.currentUsage.toFixed(2)} / ${(balance.effectiveUsageLimit ?? balance.usageLimit).toFixed(2)}`
      : '未知'
  const balancePercentUsed = balance ? `${balance.usagePercentage.toFixed(1)}% 已用` : null
  const overageEnabled = balance?.overageEnabled ?? balance?.overageStatus === 'ENABLED'
  const subscriptionLabel = credential.subscriptionTitle || balance?.subscriptionTitle || '未知'
  const subscriptionTypeLabel = credential.subscriptionType || balance?.subscriptionType || null
  const subscriptionTypeCompactLabel = formatSubscriptionTypeCompactLabel(subscriptionTypeLabel)
  const authAccountTypeLabel = formatAuthAccountTypeLabel(credential.authAccountType)
  const AuthAccountTypeIcon = getAuthAccountTypeIcon(credential.authAccountType)
  const resolvedAccountTypeLabel = credential.resolvedAccountType || '未设置'
  const resolvedAccountTypeCompactLabel = formatAccountTypeCompactLabel(credential.resolvedAccountType)
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
  const suspiciousQuarantineSummary =
    credential.suspiciousActivityQuarantineRemainingMs &&
    credential.suspiciousActivityQuarantineRemainingMs > 0
      ? `${(credential.suspiciousActivityQuarantineRemainingMs / 1000).toFixed(1)}s`
      : null
  const suspiciousQuarantineUntilSummary = credential.suspiciousActivityQuarantineUntil
    ? formatRestrictionExpiresAt(credential.suspiciousActivityQuarantineUntil)
    : null
  const suspiciousLastSeenSummary = credential.suspiciousActivityLastSeenAt
    ? formatRestrictionExpiresAt(credential.suspiciousActivityLastSeenAt)
    : null
  const hasSuspiciousActivity =
    credential.suspiciousActivityCount > 0 ||
    Boolean(credential.suspiciousActivityLastSeenAt) ||
    Boolean(suspiciousQuarantineSummary)
  const nextReadySummary =
    credential.nextReadyInMs !== undefined &&
    credential.nextReadyInMs !== null &&
    credential.nextReadyInMs > 0
      ? `${(credential.nextReadyInMs / 1000).toFixed(1)}s`
      : null
  const disabledAtSummary = credential.disabledAt
    ? formatRestrictionExpiresAt(credential.disabledAt)
    : null
  const recognizedStandardAccountType = credential.standardAccountType
    ? findStandardAccountTypePreset(credential.standardAccountType, standardAccountTypePresets)
    : null
  const credentialLabel = getCredentialLabel(credential)
  const credentialLabelWithId = getCredentialLabelWithId(credential)
  const credentialEmail = credential.email?.trim()
  const credentialEmailParts = credentialEmail ? splitEmailLabel(credentialEmail) : null
  const shouldShowCredentialId = credentialLabel !== `凭据 #${credential.id}`
  const sourceSupplierNameLabel = credential.sourceSupplierName?.trim() || null
  const sourceSupplierIdLabel = credential.sourceSupplierId?.trim() || null
  const sourceSupplierLabel = sourceSupplierNameLabel || sourceSupplierIdLabel
  const sourceSupplierTitle = [
    sourceSupplierNameLabel ? `供应商：${sourceSupplierNameLabel}` : null,
    sourceSupplierIdLabel ? `供应商 ID：${sourceSupplierIdLabel}` : null,
  ]
    .filter(Boolean)
    .join(' / ')
  const sourceBatchLabel = credential.sourceBatch?.trim() || null
  const hasSourceMetadata = Boolean(sourceSupplierLabel || sourceBatchLabel)
  const effectiveCredentialGroups = normalizeCredentialGroups(credential.credentialGroups ?? [])
  const credentialGroupLabels = effectiveCredentialGroups.length
    ? effectiveCredentialGroups
    : ['default']
  const credentialGroupTitle = `凭据分组：${credentialGroupLabels.join(', ')}`
  const totalTokens = credential.totalTokens ?? 0
  const inputTokens = credential.inputTokens ?? 0
  const outputTokens = credential.outputTokens ?? 0
  const tokenUsageCount = credential.tokenUsageCount ?? 0
  const tokenUsageTitle = `完整响应 ${formatInteger(tokenUsageCount)} 次 / 输入 ${formatInteger(inputTokens)} / 输出 ${formatInteger(outputTokens)} / 合计 ${formatInteger(totalTokens)} tokens`

  return (
    <>
      <Card
        className={cn('min-w-0 overflow-hidden', credential.isCurrent && 'ring-2 ring-primary')}
      >
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between gap-2 min-w-0">
            <div className="flex min-w-0 flex-1 items-center gap-2">
              <Checkbox
                checked={selected}
                onCheckedChange={onToggleSelect}
                className="shrink-0"
              />
              <div className="flex min-w-0 flex-1 items-center gap-1.5 overflow-hidden text-lg font-semibold leading-tight tracking-tight">
                {credentialEmailParts ? (
                  <span
                    className="flex min-w-0 flex-1 items-baseline whitespace-nowrap"
                    title={credentialLabelWithId}
                  >
                    <span className="min-w-0 truncate">{credentialEmailParts.localPart}</span>
                    <span className="shrink-0 text-muted-foreground">@</span>
                    <span className="max-w-[45%] shrink-0 truncate text-muted-foreground">
                      {credentialEmailParts.domain}
                    </span>
                  </span>
                ) : (
                  <span className="block min-w-0 flex-1 truncate" title={credentialLabelWithId}>
                    {credentialLabel}
                  </span>
                )}
                {shouldShowCredentialId && (
                  <Badge variant="outline" className="shrink-0">
                    #{credential.id}
                  </Badge>
                )}
                {credential.isCurrent && (
                  <Badge variant="success" className="shrink-0">
                    当前
                  </Badge>
                )}
                {credential.disabled && (
                  <Badge
                    variant="destructive"
                    className="shrink-0"
                    title={
                      credential.disabledReason
                        ? `已禁用：${credential.disabledReason}`
                        : '已禁用'
                    }
                  >
                    禁用
                  </Badge>
                )}
                {hasSuspiciousActivity && (
                  <Badge
                    variant={suspiciousQuarantineSummary ? 'warning' : 'outline'}
                    className="shrink-0"
                    title={`Suspicious activity ${credential.suspiciousActivityCount}`}
                  >
                    S{credential.suspiciousActivityCount}
                  </Badge>
                )}
              </div>
            </div>
            <div className="flex items-center gap-1.5 shrink-0 text-sm font-normal text-muted-foreground">
              <span>启用</span>
              <Switch
                checked={!credential.disabled}
                onCheckedChange={handleToggleDisabled}
                disabled={setDisabled.isPending}
              />
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          {/* 订阅用量与进度条 */}
          <div className="space-y-1.5">
            <div className="flex items-center justify-between text-xs">
              <div className="flex items-center gap-1.5 font-medium text-foreground min-w-0">
                <Wallet className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                {loadingBalance && !credential.subscriptionTitle ? (
                  <Loader2 className="h-3.5 w-3.5 shrink-0 animate-spin" />
                ) : (
                  <span className="truncate" title={subscriptionLabel}>
                    {subscriptionLabel}
                  </span>
                )}
                {subscriptionTypeCompactLabel && subscriptionTypeLabel !== subscriptionLabel && (
                  <CompactPill
                    icon={Layers}
                    label={subscriptionTypeCompactLabel}
                    title={`内部订阅类型：${subscriptionTypeLabel}`}
                    tone="accent"
                  />
                )}
                {overageEnabled && (
                  <Badge variant="warning" className="shrink-0 px-1.5 py-0 text-[10px] h-4 leading-none">
                    <Zap className="mr-0.5 h-2.5 w-2.5" />
                    超额
                  </Badge>
                )}
              </div>
              <div className="text-muted-foreground font-mono text-right shrink-0">
                {loadingBalance ? (
                  <Loader2 className="h-3 w-3 animate-spin inline" />
                ) : balance ? (
                  <span title={balancePercentUsed ? `${balanceSummary} (${balancePercentUsed})` : balanceSummary ?? undefined}>
                    {balanceSummary}
                    {balancePercentUsed && <span className="ml-1 text-[10px] text-primary">({balancePercentUsed})</span>}
                  </span>
                ) : (
                  '未知余额'
                )}
              </div>
            </div>
            {balance && (
              <Progress value={balance.usagePercentage} className="h-1.5 bg-muted" />
            )}
          </div>

          {/* 双列无边框网格 */}
          <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-xs py-2 border-y border-muted-foreground/10">
            {/* 左列：配置与类型 (Configs) */}
            <div className="space-y-2 min-w-0">
              <div className="flex items-center justify-between gap-2">
                <span className="text-muted-foreground shrink-0">优先级</span>
                <span className="font-semibold text-foreground truncate">{credential.priority}</span>
              </div>
              <div className="space-y-1">
                <div className="text-muted-foreground">生效与认证</div>
                <div className="flex flex-wrap gap-1">
                  <CompactPill
                    icon={ShieldCheck}
                    label={resolvedAccountTypeCompactLabel}
                    title={`生效账号类型：${resolvedAccountTypeLabel}`}
                    tone={credential.resolvedAccountType ? 'success' : 'muted'}
                  />
                  <CompactPill
                    icon={AuthAccountTypeIcon}
                    label={authAccountTypeLabel}
                    title={`认证类型：${authAccountTypeLabel}`}
                  />
                  {accountTypeSourceShortLabel && (
                    <CompactPill
                      icon={Layers}
                      label={accountTypeSourceShortLabel}
                      title={accountTypeSourceLabel ?? accountTypeSourceShortLabel}
                    />
                  )}
                </div>
              </div>
            </div>

            {/* 右列：运行指标与状态 (Metrics) */}
            <div className="space-y-2 min-w-0 border-l pl-4 border-muted-foreground/10">
              <div className="space-y-1">
                <div className="flex items-center justify-between gap-2">
                  <span className="text-muted-foreground shrink-0">并发状态</span>
                  <span className="font-semibold text-foreground truncate" title="当前并发 / 并发上限">
                    {credential.inFlight} / {credential.maxConcurrency ?? '不限'}
                  </span>
                </div>
                {maxConcurrencySourceLabel && (
                  <div
                    className="truncate text-[10px] text-muted-foreground text-right"
                    title={`并发上限来源：${maxConcurrencySourceLabel}`}
                  >
                    ({maxConcurrencySourceLabel})
                  </div>
                )}
              </div>

              <div className="flex items-center justify-between gap-2">
                <span className="text-muted-foreground shrink-0">最近调用</span>
                <div className="flex items-center gap-1 font-semibold text-foreground truncate">
                  <Clock3 className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                  <span title={formatLastUsed(credential.lastUsedAt)} className="truncate">
                    {formatLastUsed(credential.lastUsedAt)}
                  </span>
                </div>
              </div>
            </div>
          </div>

          {/* 限速覆盖区域 */}
          <div className="rounded-md bg-muted/30 px-2.5 py-1.5 text-xs text-muted-foreground">
            <div className="font-semibold text-[10px] uppercase tracking-wider text-muted-foreground/80">限速规则覆盖</div>
            <div className="mt-0.5 font-medium text-foreground break-all leading-normal">
              {rateLimitOverrideSummary}
            </div>
          </div>

          {/* 监控指标统计行 */}
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5 text-xs border-t pt-2 text-muted-foreground">
            <div className="flex items-center gap-1">
              <span className="inline-block w-2 h-2 rounded-full bg-green-500" />
              <span>成功: <span className="font-semibold text-foreground">{credential.successCount}</span></span>
            </div>
            <div className="flex items-center gap-1" title={tokenUsageTitle}>
              <span className="inline-block h-2 w-2 rounded-full bg-sky-500" />
              <span>Token: <span className="font-semibold text-foreground">{formatTokenCount(totalTokens)}</span></span>
            </div>
            <div className="flex items-center gap-1">
              <span className={cn("inline-block w-2 h-2 rounded-full", credential.failureCount > 0 ? "bg-red-500" : "bg-muted")} />
              <span>失败: <span className={cn("font-semibold", credential.failureCount > 0 ? "text-destructive" : "text-foreground")}>{credential.failureCount}</span></span>
            </div>
            {credential.refreshFailureCount > 0 && (
              <div className="flex items-center gap-1 text-destructive">
                <span className="inline-block w-2 h-2 rounded-full bg-red-500" />
                <span>刷新失败: <span className="font-semibold">{credential.refreshFailureCount}</span></span>
              </div>
            )}
            {credential.rateLimitHitStreak > 0 && (
              <Badge variant="warning" className="px-1.5 py-0 text-[10px]">
                连续 429: {credential.rateLimitHitStreak}
              </Badge>
            )}
          </div>

          {/* 限制与警示高亮 */}
          {(cooldownSummary || suspiciousQuarantineSummary || nextReadySummary) && (
            <div className="flex flex-wrap gap-1.5">
              {cooldownSummary && <Badge variant="warning">429 冷却 {cooldownSummary}</Badge>}
              {suspiciousQuarantineSummary && (
                <Badge variant="warning">Suspicious 隔离 {suspiciousQuarantineSummary}</Badge>
              )}
              {nextReadySummary && <Badge variant="outline">下次可调度 {nextReadySummary}</Badge>}
            </div>
          )}

          {/* 静态配置标签 */}
          <div className="flex flex-wrap gap-1.5">
            {sourceSupplierLabel && (
              <Badge
                variant="secondary"
                className="max-w-full truncate px-1.5 py-0 text-[10px]"
                title={sourceSupplierTitle || sourceSupplierLabel}
              >
                <Building2 className="mr-1 h-3 w-3 shrink-0" />
                供应商: {sourceSupplierLabel}
              </Badge>
            )}
            {sourceBatchLabel && (
              <Badge
                variant="outline"
                className="max-w-full truncate px-1.5 py-0 text-[10px]"
                title={`批次：${sourceBatchLabel}`}
              >
                <Layers className="mr-1 h-3 w-3 shrink-0" />
                批次: {sourceBatchLabel}
              </Badge>
            )}
            <Badge
              variant="secondary"
              className="max-w-full truncate px-1.5 py-0 text-[10px]"
              title={credentialGroupTitle}
            >
              <Tags className="mr-1 h-3 w-3 shrink-0" />
              分组: {credentialGroupLabels.slice(0, 3).join(', ')}
              {credentialGroupLabels.length > 3 ? ` +${credentialGroupLabels.length - 3}` : ''}
            </Badge>
            {!hasSourceMetadata && (
              <Badge variant="outline" className="px-1.5 py-0 text-[10px] text-muted-foreground">
                来源未标记
              </Badge>
            )}
            <Badge variant="secondary" className="text-[10px] px-1.5 py-0">
              策略: {policySummary}
            </Badge>
            {bucketSummary && <Badge variant="outline" className="text-[10px] px-1.5 py-0">Bucket {bucketSummary}</Badge>}
            {refillSummary && <Badge variant="outline" className="text-[10px] px-1.5 py-0">回填 {refillSummary}</Badge>}
            {credential.hasProxy && <Badge variant="outline" className="text-[10px] px-1.5 py-0">代理已配</Badge>}
            {compactProfileLabel && (
              <Badge variant="secondary" className="text-[10px] px-1.5 py-0 max-w-full truncate" title={savedProfileArn}>
                Profile: {compactProfileLabel}
              </Badge>
            )}
            {recognizedStandardAccountType && (
              <Badge variant="outline" className="text-[10px] px-1.5 py-0 max-w-full truncate" title={recognizedStandardAccountType.preset.displayName}>
                档位: {recognizedStandardAccountType.preset.displayName}
              </Badge>
            )}
          </div>

          {hasSuspiciousActivity && (
            <div className="space-y-1 rounded-lg border border-dashed border-amber-300 bg-amber-500/5 px-3 py-2 text-xs">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex flex-wrap items-center gap-2 text-muted-foreground">
                  <Badge variant={suspiciousQuarantineSummary ? 'warning' : 'outline'}>
                    suspicious activity {credential.suspiciousActivityCount}
                  </Badge>
                  <Badge variant="outline">
                    恢复成功 {credential.suspiciousActivityRecoverySuccessCount}
                  </Badge>
                  {suspiciousLastSeenSummary && <span>最近命中 {suspiciousLastSeenSummary}</span>}
                  {suspiciousQuarantineUntilSummary && (
                    <span>隔离至 {suspiciousQuarantineUntilSummary}</span>
                  )}
                </div>
                <Button
                  size="sm"
                  variant="outline"
                  className="h-7 px-2 text-xs"
                  onClick={handleClearSuspiciousActivity}
                  disabled={clearSuspiciousActivity.isPending}
                  title="清除 suspicious activity 标记与隔离"
                >
                  {clearSuspiciousActivity.isPending ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <Eraser className="h-3.5 w-3.5" />
                  )}
                  清除
                </Button>
              </div>
            </div>
          )}

          {hasRuntimeRestrictions && (
            <details className="rounded-lg border border-dashed border-amber-300 bg-amber-500/5 px-3 py-2 text-xs cursor-pointer">
              <summary className="flex items-center justify-between gap-2 font-medium text-muted-foreground outline-none">
                <span>运行时临时限制 ({credential.runtimeModelRestrictions?.length ?? 0} 条)</span>
                <Button
                  size="sm"
                  variant="outline"
                  className="h-6 px-1.5 text-[11px]"
                  onClick={(e) => {
                    e.stopPropagation()
                    handleClearRuntimeModelRestrictions()
                  }}
                  disabled={clearRuntimeModelCooldown.isPending}
                  title="清除模型冷却"
                >
                  {clearRuntimeModelCooldown.isPending ? (
                    <Loader2 className="h-3 w-3 animate-spin" />
                  ) : (
                    <RefreshCw className="h-3 w-3" />
                  )}
                  清除
                </Button>
              </summary>
              <div className="flex flex-wrap gap-1.5 mt-2">
                {credential.runtimeModelRestrictions?.map((restriction) => (
                  <Badge key={`${restriction.model}-${restriction.expiresAt}`} variant="outline" className="text-[10px]">
                    {restriction.model} 至 {formatRestrictionExpiresAt(restriction.expiresAt)}
                  </Badge>
                ))}
              </div>
            </details>
          )}

          <div className="flex items-center justify-between gap-3 rounded-lg border border-dashed bg-muted/10 px-3 py-2 text-xs">
            <div className="min-w-0 space-y-1">
              <div className="flex items-center gap-2">
                <Network className="h-3.5 w-3.5 text-muted-foreground" />
                <Badge
                  variant={proxySummary.tone === 'success' ? 'success' : proxySummary.tone === 'accent' ? 'default' : 'outline'}
                  className="max-w-[220px] truncate"
                >
                  {proxySummary.label}
                </Badge>
              </div>
              <div className="truncate text-muted-foreground" title={proxySummary.detail}>
                {proxySummary.detail}
              </div>
            </div>
            <Button
              type="button"
              size="sm"
              variant="outline"
              className="h-7 shrink-0 px-2 text-xs"
              onClick={openProxyDialog}
              disabled={isSaving}
            >
              代理
            </Button>
          </div>

          {credential.disabled && (credential.lastErrorSummary || disabledAtSummary) && (
            <div className="space-y-1 rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs">
              <div className="flex flex-wrap items-center gap-2 text-muted-foreground">
                {credential.lastErrorStatus && (
                  <Badge variant="destructive">HTTP {credential.lastErrorStatus}</Badge>
                )}
                {disabledAtSummary && <span>停调时间 {disabledAtSummary}</span>}
              </div>
              {credential.lastErrorSummary && (
                <div className="break-words text-foreground">{credential.lastErrorSummary}</div>
              )}
            </div>
          )}

          {/* 操作按钮 */}
          <div className="flex items-center gap-2 pt-2 border-t mt-2">
            <Button
              size="sm"
              variant="default"
              className="flex-1"
              onClick={() => onViewBalance(credential.id)}
            >
              <Wallet className="h-4 w-4 mr-1" />
              查看余额
            </Button>
            <Button
              size="sm"
              variant="outline"
              className="flex-1"
              onClick={openModelPolicyDialog}
              disabled={isSaving}
            >
              配置与策略
            </Button>

            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button size="sm" variant="outline" className="px-2 shrink-0" disabled={isSaving}>
                  <MoreVertical className="h-4 w-4" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-40">
                <DropdownMenuItem
                  onClick={handleReset}
                  disabled={resetFailure.isPending || (credential.failureCount === 0 && credential.refreshFailureCount === 0)}
                >
                  <RefreshCw className="h-4 w-4 mr-2 text-muted-foreground" />
                  重置失败
                </DropdownMenuItem>
                <DropdownMenuItem
                  onClick={handleForceRefresh}
                  disabled={forceRefresh.isPending || credential.disabled}
                >
                  <RefreshCw className="h-4 w-4 mr-2 text-muted-foreground" />
                  刷新 Token
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem
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
                  <ChevronUp className="h-4 w-4 mr-2 text-muted-foreground" />
                  提高优先级
                </DropdownMenuItem>
                <DropdownMenuItem
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
                  <ChevronDown className="h-4 w-4 mr-2 text-muted-foreground" />
                  降低优先级
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem
                  className="text-destructive focus:text-destructive focus:bg-destructive/10"
                  onClick={() => setShowDeleteDialog(true)}
                  disabled={!credential.disabled}
                >
                  <Trash2 className="h-4 w-4 mr-2" />
                  删除凭据
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </CardContent>
      </Card>

      {/* 统一的配置与策略对话框 */}
      <Dialog open={showModelPolicyDialog} onOpenChange={setShowModelPolicyDialog}>
        <DialogContent className="sm:max-w-3xl max-h-[85vh] flex flex-col">
          <DialogHeader>
            <DialogTitle>编辑凭据配置与策略</DialogTitle>
            <DialogDescription>
              在此统一配置凭据的基础属性、Kiro Profile、限速规则覆盖及模型访问策略。
            </DialogDescription>
          </DialogHeader>
          <div className="flex-1 space-y-4 overflow-y-auto pr-1">

            {/* 基础信息配置区 */}
            <div className="grid grid-cols-2 gap-4 rounded-lg border p-3 bg-muted/5">
              <div className="space-y-1.5">
                <label htmlFor={`priority-${credential.id}`} className="text-sm font-medium">优先级</label>
                <Input
                  id={`priority-${credential.id}`}
                  type="number"
                  value={priorityValue}
                  onChange={(e) => setPriorityValue(e.target.value)}
                  className="h-9 text-sm"
                  min="0"
                  disabled={isSaving}
                />
                <p className="text-[10px] text-muted-foreground">数值越小，调度优先级越高</p>
              </div>

              <div className="space-y-1.5">
                <label htmlFor={`concurrency-${credential.id}`} className="text-sm font-medium">并发上限</label>
                <Input
                  id={`concurrency-${credential.id}`}
                  type="number"
                  value={maxConcurrencyValue}
                  onChange={(e) => setMaxConcurrencyValue(e.target.value)}
                  className="h-9 text-sm"
                  min="1"
                  placeholder={maxConcurrencyPlaceholder}
                  disabled={isSaving}
                />
                <p className="text-[10px] text-muted-foreground">留空表示清除凭据覆盖，继续跟随默认策略</p>
              </div>
            </div>

            {/* 来源标记配置区 */}
            <div className="space-y-2 rounded-lg border p-3 bg-muted/5">
              <div className="text-sm font-medium text-foreground">来源标记</div>
              <div className="grid gap-3 sm:grid-cols-3">
                <div className="space-y-1">
                  <label htmlFor={`source-supplier-name-${credential.id}`} className="text-xs text-muted-foreground">
                    供应商
                  </label>
                  <Input
                    id={`source-supplier-name-${credential.id}`}
                    value={sourceSupplierNameValue}
                    onChange={(e) => setSourceSupplierNameValue(e.target.value)}
                    className="h-9 text-sm"
                    placeholder="供应商名称"
                    disabled={isSaving}
                  />
                </div>
                <div className="space-y-1">
                  <label htmlFor={`source-supplier-id-${credential.id}`} className="text-xs text-muted-foreground">
                    供应商 ID
                  </label>
                  <Input
                    id={`source-supplier-id-${credential.id}`}
                    value={sourceSupplierIdValue}
                    onChange={(e) => setSourceSupplierIdValue(e.target.value)}
                    className="h-9 text-sm"
                    placeholder="可选"
                    disabled={isSaving}
                  />
                </div>
                <div className="space-y-1">
                  <label htmlFor={`source-batch-${credential.id}`} className="text-xs text-muted-foreground">
                    批次
                  </label>
                  <Input
                    id={`source-batch-${credential.id}`}
                    value={sourceBatchValue}
                    onChange={(e) => setSourceBatchValue(e.target.value)}
                    className="h-9 text-sm"
                    placeholder="如 20260618211"
                    disabled={isSaving}
                  />
                </div>
              </div>
              <p className="text-[10px] text-muted-foreground">留空并保存会清除对应来源字段。</p>
            </div>

            {/* 凭据分组配置区 */}
            <div className="space-y-2 rounded-lg border p-3 bg-muted/5">
              <div className="text-sm font-medium text-foreground">凭据分组</div>
              <CredentialGroupPicker
                id={`credential-groups-${credential.id}`}
                value={credentialGroupsValue}
                onChange={setCredentialGroupsValue}
                disabled={isSaving}
                compact
              />
            </div>

            {/* Profile 选择配置区 */}
            <div className="space-y-2 rounded-lg border p-3 bg-muted/5">
              <div className="text-sm font-medium text-foreground">Kiro Profile 设定</div>
              {profileQuery.isLoading && (
                <div className="flex items-center gap-2 text-xs text-muted-foreground py-1">
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  正在加载可选 Profile...
                </div>
              )}
              {profileQuery.error && (
                <div className="text-xs text-destructive py-1">
                  加载失败：{(profileQuery.error as Error).message}
                </div>
              )}
              {!profileQuery.isLoading && !profileQuery.error && profileSelectOptions.length === 0 && (
                <div className="text-xs text-muted-foreground py-1">
                  当前凭据没有可选的 Kiro Profile
                </div>
              )}
              {profileSelectOptions.length > 0 && (
                <div className="space-y-3">
                  <div className="space-y-1">
                    <select
                      id={`profile-select-${credential.id}`}
                      value={selectedProfileValue}
                      onChange={(event) => {
                        setSelectedProfileArn(event.target.value)
                        setProfileSelectionTouched(true)
                      }}
                      disabled={isSaving}
                      className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      {!savedProfileArn && <option value="">保持不修改 Profile</option>}
                      {profileSelectOptions.map((profile) => (
                        <option key={profile.arn} value={profile.arn}>
                          {formatProfileOptionLabel(profile)}
                        </option>
                      ))}
                    </select>
                  </div>
                  {selectedProfileValue && (
                    <div className="space-y-1">
                      <div className="text-[10px] text-muted-foreground font-mono truncate" title={selectedProfileValue}>
                        Selected ARN: {selectedProfileValue}
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>

            {/* 限速覆盖配置区 */}
            <div className="space-y-2 rounded-lg border border-dashed p-3 bg-muted/5">
              <div className="text-sm font-medium">凭据级限速配置覆盖</div>
              <div className="grid gap-4 md:grid-cols-3">
                <div className="space-y-1">
                  <label htmlFor={`rate-limit-cooldown-${credential.id}`} className="text-xs text-muted-foreground">429 退避</label>
                  <select
                    id={`rate-limit-cooldown-${credential.id}`}
                    value={rateLimitCooldownMode}
                    onChange={(e) => setRateLimitCooldownMode(e.target.value as RateLimitCooldownMode)}
                    disabled={isSaving}
                    className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    <option value="global">跟随全局</option>
                    <option value="enabled">强制开启</option>
                    <option value="disabled">强制关闭</option>
                  </select>
                </div>
                <div className="space-y-1">
                  <label htmlFor={`bucket-${credential.id}`} className="text-xs text-muted-foreground">Bucket 容量</label>
                  <Input
                    id={`bucket-${credential.id}`}
                    type="number"
                    value={bucketCapacityValue}
                    onChange={(e) => setBucketCapacityValue(e.target.value)}
                    className="h-9 text-sm"
                    min="0"
                    step="0.1"
                    placeholder="不限"
                    disabled={isSaving}
                  />
                </div>
                <div className="space-y-1">
                  <label htmlFor={`refill-${credential.id}`} className="text-xs text-muted-foreground">回填速率 (token/s)</label>
                  <Input
                    id={`refill-${credential.id}`}
                    type="number"
                    value={refillPerSecondValue}
                    onChange={(e) => setRefillPerSecondValue(e.target.value)}
                    className="h-9 text-sm"
                    min="0"
                    step="0.1"
                    placeholder="不限"
                    disabled={isSaving}
                  />
                </div>
              </div>
              <p className="text-[10px] text-muted-foreground">
                429 退避留空跟随全局；Bucket/回填留空跟随全局或账号类型配置，输入 0 会针对此账号禁用 token bucket 限制。
              </p>
            </div>

            {/* 模型策略配置区 */}
            <div className="space-y-3 rounded-lg border p-3 bg-muted/5">
              <div className="text-sm font-medium">模型调度策略与账号类型</div>

              <AccountTypeInput
                id={`account-type-${credential.id}`}
                label="账号类型"
                value={accountTypeValue}
                onChange={setAccountTypeValue}
                suggestions={accountTypeSuggestions}
                standardAccountTypePresets={standardAccountTypePresets}
                placeholder="优先选择已有类型，也可直接新建"
                disabled={isSaving}
              />

              <div className="grid gap-3 lg:grid-cols-2 mt-2">
                <details className="rounded-lg border border-input bg-muted/10 p-2">
                  <summary className="cursor-pointer text-xs font-medium outline-none">
                    <div className="flex items-center justify-between">
                      <span>账号级额外允许模型 ({allowedModelsValue.length} 已选)</span>
                    </div>
                    <p className="text-[10px] text-muted-foreground mt-0.5 normal-case font-normal truncate max-w-full">
                      {allowedModelsSummary}
                    </p>
                  </summary>
                  <div className="mt-2">
                    <ModelSelector
                      label="账号级额外允许模型"
                      selectedValues={allowedModelsValue}
                      onChange={setAllowedModelsValue}
                      options={modelCatalog}
                      hideHeader
                    />
                  </div>
                </details>

                <details className="rounded-lg border border-input bg-muted/10 p-2">
                  <summary className="cursor-pointer text-xs font-medium outline-none">
                    <div className="flex items-center justify-between">
                      <span>账号级额外禁用模型 ({blockedModelsValue.length} 已选)</span>
                    </div>
                    <p className="text-[10px] text-muted-foreground mt-0.5 normal-case font-normal truncate max-w-full">
                      {blockedModelsSummary}
                    </p>
                  </summary>
                  <div className="mt-2">
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
            </div>

            <div className="flex items-center gap-2 rounded-lg border border-dashed p-3">
              <Checkbox
                checked={clearRuntimeModelRestrictions}
                onCheckedChange={(checked) => setClearRuntimeModelRestrictions(Boolean(checked))}
                disabled={isSaving}
              />
              <div className="space-y-0.5">
                <div className="text-sm font-medium">保存时清空运行时临时限制</div>
                <p className="text-xs text-muted-foreground">
                  当前该凭据共有 {credential.runtimeModelRestrictions?.length ?? 0} 条临时冷却限制。
                </p>
              </div>
            </div>
          </div>
          <DialogFooter className="border-t pt-3">
            <Button
              variant="outline"
              onClick={() => setShowModelPolicyDialog(false)}
              disabled={isSaving}
            >
              取消
            </Button>
            <Button onClick={handleModelPolicySave} disabled={isSaving}>
              {isSaving && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
              保存
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* 代理配置对话框 */}
      <Dialog open={showProxyDialog} onOpenChange={setShowProxyDialog}>
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>编辑凭据代理</DialogTitle>
            <DialogDescription>
              当前凭据：{credentialLabelWithId}
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4">
            <div className="grid gap-2 sm:grid-cols-2">
              {[
                {
                  mode: 'auto' as const,
                  icon: Shuffle,
                  label: '自动均衡',
                  desc: '立即按代理池策略重新分配',
                },
                {
                  mode: 'pool' as const,
                  icon: Server,
                  label: '指定节点',
                  desc: '固定绑定到一个代理池节点',
                },
                {
                  mode: 'custom' as const,
                  icon: Link2,
                  label: '自定义代理',
                  desc: '使用凭据级 HTTP/SOCKS5 代理',
                },
                {
                  mode: 'global' as const,
                  icon: Network,
                  label: '跟随全局',
                  desc: '清空凭据级代理配置',
                },
              ].map(({ mode, icon: Icon, label, desc }) => {
                const disabled =
                  isSaving ||
                  (mode === 'pool' && (!proxyPoolEnabled || proxyPoolOptions.length === 0)) ||
                  (mode === 'global' && proxyRequireProxy)
                return (
                  <button
                    key={mode}
                    type="button"
                    onClick={() => setProxyMode(mode)}
                    disabled={disabled}
                    className={cn(
                      'rounded-md border p-3 text-left transition-colors',
                      proxyMode === mode
                        ? 'border-primary bg-primary/10 text-primary'
                        : 'border-input bg-background hover:bg-muted/60',
                      disabled && 'cursor-not-allowed opacity-50'
                    )}
                  >
                    <div className="flex items-center gap-2 text-sm font-medium">
                      <Icon className="h-4 w-4" />
                      {label}
                    </div>
                    <div className="mt-1 text-xs text-muted-foreground">{desc}</div>
                  </button>
                )
              })}
              <button
                type="button"
                onClick={() => setProxyMode('direct')}
                disabled={isSaving || proxyRequireProxy}
                className={cn(
                  'rounded-md border p-3 text-left transition-colors sm:col-span-2',
                  proxyMode === 'direct'
                    ? 'border-primary bg-primary/10 text-primary'
                    : 'border-input bg-background hover:bg-muted/60',
                  (isSaving || proxyRequireProxy) && 'cursor-not-allowed opacity-50'
                )}
              >
                <div className="flex items-center gap-2 text-sm font-medium">
                  <Globe2 className="h-4 w-4" />
                  直连
                </div>
                <div className="mt-1 text-xs text-muted-foreground">
                  显式绕过代理池和全局代理；启用强制代理时不可用
                </div>
              </button>
            </div>

            {proxyMode === 'auto' && (
              <div className="rounded-md border border-dashed bg-muted/10 p-3 text-sm">
                <div className="font-medium">自动均衡分配</div>
                <div className="mt-1 text-xs text-muted-foreground">
                  保存后后端会根据当前代理池绑定数量和权重立即选择一个节点，并写回该凭据。
                </div>
                {!proxyPoolEnabled && (
                  <div className="mt-2 text-xs text-amber-600">
                    当前代理池未启用，保存后会清空凭据级代理并跟随全局配置。
                  </div>
                )}
              </div>
            )}

            {proxyMode === 'pool' && (
              <div className="space-y-2">
                <label htmlFor={`proxy-id-${credential.id}`} className="text-sm font-medium">
                  代理池节点
                </label>
                <select
                  id={`proxy-id-${credential.id}`}
                  value={proxyIdValue}
                  onChange={(e) => setProxyIdValue(e.target.value)}
                  disabled={isSaving || proxyPoolOptions.length === 0}
                  className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  <option value="">选择代理池节点</option>
                  {proxyPoolOptions.map((proxy) => (
                    <option key={proxy.id} value={proxy.id}>
                      {proxyPoolEntryLabel(proxy)}
                    </option>
                  ))}
                </select>
                {proxyPoolOptions.length === 0 && (
                  <div className="text-xs text-amber-600">当前没有启用的代理池节点。</div>
                )}
              </div>
            )}

            {proxyMode === 'custom' && (
              <div className="space-y-3">
                <div className="space-y-2">
                  <label htmlFor={`proxy-url-${credential.id}`} className="text-sm font-medium">
                    代理 URL
                  </label>
                  <Input
                    id={`proxy-url-${credential.id}`}
                    placeholder="http://proxy:3128 或 socks5://proxy:1080"
                    value={proxyUrlValue}
                    onChange={(e) => setProxyUrlValue(e.target.value)}
                    disabled={isSaving}
                  />
                </div>
                <div className="grid gap-2 sm:grid-cols-2">
                  <Input
                    placeholder="用户名"
                    value={proxyUsernameValue}
                    onChange={(e) => setProxyUsernameValue(e.target.value)}
                    disabled={isSaving}
                  />
                  <Input
                    type="password"
                    placeholder="密码"
                    value={proxyPasswordValue}
                    onChange={(e) => setProxyPasswordValue(e.target.value)}
                    disabled={isSaving}
                  />
                </div>
                <div className="text-xs text-muted-foreground">
                  出于安全考虑，已保存的代理密码不会回显；修改自定义代理时需要重新填写认证信息。
                </div>
              </div>
            )}

            {proxyRequireProxy && (
              <div className="rounded-md border border-amber-300 bg-amber-500/5 px-3 py-2 text-xs text-amber-700">
                当前代理池启用了强制代理，不能保存为跟随全局或直连。
              </div>
            )}
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => setShowProxyDialog(false)}
              disabled={isSaving}
            >
              取消
            </Button>
            <Button type="button" onClick={handleProxySave} disabled={isSaving}>
              {setProxy.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
              保存代理
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
              您确定要删除 {credentialLabelWithId} 吗？此操作无法撤销。
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
