import { useMemo, useRef, useState } from 'react'
import { toast } from 'sonner'
import { CheckCircle2, XCircle, AlertCircle, Loader2, Globe2, Link2, Network, Server, Shuffle, Tags } from 'lucide-react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import { useCredentials, useAddCredential, useDeleteCredential, useLoadBalancingMode } from '@/hooks/use-credentials'
import { getCredentialBalance, setCredentialDisabled, setCredentialOverageStatus } from '@/api/credentials'
import type { CredentialProxyMode, ProxyPoolEntry } from '@/types/api'
import { cn, extractErrorMessage, sha256Hex } from '@/lib/utils'
import {
  collectSourceSupplierSuggestions,
  formatDefaultSourceBatch,
} from '@/lib/source-metadata'

interface KamImportDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

// KAM 导出 JSON 中的账号结构
interface KamAccount {
  email?: string
  userId?: string | null
  provider?: string
  tokenEndpoint?: string
  issuerUrl?: string
  scopes?: string | string[]
  audience?: string
  nickname?: string
  profileArn?: string
  accountType?: string
  sourceSupplierId?: string
  sourceSupplierName?: string
  sourceBatch?: string
  source_supplier_id?: string
  source_supplier_name?: string
  source_batch?: string
  availableModelIds?: string[]
  availableModels?: unknown[]
  models?: unknown[]
  availableModelsCache?: {
    response?: unknown
    models?: unknown[]
    cachedAt?: number
    modelProvider?: string | null
  } | null
  maxConcurrency?: number
  rateLimitCooldownEnabled?: boolean
  rateLimitBucketCapacity?: number
  rateLimitRefillPerSecond?: number
  priority?: number
  proxyId?: string
  proxyUrl?: string
  proxyUsername?: string
  proxyPassword?: string
  credentials: {
    refreshToken: string
    clientId?: string
    clientSecret?: string
    region?: string
    authRegion?: string
    apiRegion?: string
    profileArn?: string
    provider?: string
    authMethod?: string
    tokenEndpoint?: string
    issuerUrl?: string
    scopes?: string | string[]
    audience?: string
    startUrl?: string
    sourceSupplierId?: string
    sourceSupplierName?: string
    sourceBatch?: string
    source_supplier_id?: string
    source_supplier_name?: string
    source_batch?: string
    maxConcurrency?: number
    rateLimitCooldownEnabled?: boolean
    rateLimitBucketCapacity?: number
    rateLimitRefillPerSecond?: number
    priority?: number
    proxyId?: string
    proxyUrl?: string
    proxyUsername?: string
    proxyPassword?: string
  }
  machineId?: string
  status?: string
}

interface VerificationResult {
  index: number
  status: 'pending' | 'checking' | 'verifying' | 'verified' | 'duplicate' | 'failed' | 'skipped'
  error?: string
  usage?: string
  email?: string
  credentialId?: number
  rollbackStatus?: 'success' | 'failed' | 'skipped'
  rollbackError?: string
}

type RateLimitCooldownMode = 'global' | 'enabled' | 'disabled'

const KAM_DEFAULT_AUTH_REGION = 'us-east-1'

function rateLimitCooldownValueFromMode(mode: RateLimitCooldownMode): boolean | undefined {
  if (mode === 'enabled') return true
  if (mode === 'disabled') return false
  return undefined
}

function proxyPoolEntryLabel(proxy: ProxyPoolEntry): string {
  const egress = proxy.expectedEgressIp ? ` (${proxy.expectedEgressIp})` : ''
  const assigned = typeof proxy.assignedCredentials === 'number'
    ? ` · 已挂载 ${proxy.assignedCredentials} 凭据`
    : ''
  return `${proxy.id}${egress}${assigned}`
}

function getString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined
}

function getAliasedString(obj: unknown, ...keys: string[]): string | undefined {
  if (typeof obj !== 'object' || obj === null) return undefined
  const record = obj as Record<string, unknown>
  for (const key of keys) {
    const value = getString(record[key])?.trim()
    if (value) return value
  }
  return undefined
}

function getNumber(value: unknown): number | undefined {
  return typeof value === 'number' ? value : undefined
}

function getBoolean(value: unknown): boolean | undefined {
  return typeof value === 'boolean' ? value : undefined
}

function getKamSourceString(
  account: KamAccount,
  key: 'sourceSupplierId' | 'sourceSupplierName' | 'sourceBatch',
  snakeKey: 'source_supplier_id' | 'source_supplier_name' | 'source_batch'
): string | undefined {
  return (
    getAliasedString(account.credentials, key, snakeKey) ??
    getAliasedString(account, key, snakeKey)
  )
}

function getKamDispatchNumber(
  account: KamAccount,
  key: 'maxConcurrency' | 'rateLimitBucketCapacity' | 'rateLimitRefillPerSecond'
): number | undefined {
  return getNumber(account[key]) ?? getNumber(account.credentials[key])
}

function getKamDispatchBoolean(
  account: KamAccount,
  key: 'rateLimitCooldownEnabled'
): boolean | undefined {
  return getBoolean(account[key]) ?? getBoolean(account.credentials[key])
}

function hasInvalidKamDispatchBoolean(
  account: KamAccount,
  key: 'rateLimitCooldownEnabled'
): boolean {
  const topLevel = account[key]
  const nested = account.credentials[key]
  return (
    (topLevel !== undefined && typeof topLevel !== 'boolean') ||
    (nested !== undefined && typeof nested !== 'boolean')
  )
}

function extractModelId(value: unknown): string | undefined {
  if (typeof value !== 'object' || value === null) return undefined
  return getString((value as Record<string, unknown>).modelId)
}

function extractAvailableModelIds(account: Pick<KamAccount, 'availableModelIds' | 'availableModels' | 'models' | 'availableModelsCache'>): string[] {
  const ids = new Set<string>()
  for (const modelId of account.availableModelIds || []) {
    const trimmed = modelId.trim()
    if (trimmed) ids.add(trimmed)
  }

  const topLevelModels = [
    ...(account.availableModels || []),
    ...(account.models || []),
    ...(account.availableModelsCache?.models || []),
  ]
  for (const model of topLevelModels) {
    const modelId = extractModelId(model)?.trim()
    if (modelId) ids.add(modelId)
  }

  const response = account.availableModelsCache?.response
  if (typeof response === 'object' && response !== null) {
    const obj = response as Record<string, unknown>
    const models = Array.isArray(obj.availableModels)
      ? obj.availableModels
      : Array.isArray(obj.models)
        ? obj.models
        : []
    for (const model of models) {
      const modelId = extractModelId(model)?.trim()
      if (modelId) ids.add(modelId)
    }
    const defaultModelId = extractModelId(obj.defaultModel)?.trim()
    if (defaultModelId) ids.add(defaultModelId)
  }

  return [...ids]
}

function normalizeProvider(provider?: string): string | undefined {
  const trimmed = provider?.trim()
  if (!trimmed) return undefined
  const lower = trimmed.toLowerCase()
  if (lower === 'enterprise') return 'Enterprise'
  if (lower === 'externalidp' || lower === 'external-idp' || lower === 'external_idp' || lower === 'external idp') return 'ExternalIdp'
  if (lower === 'builderid' || lower === 'builder-id' || lower === 'builder_id') return 'BuilderId'
  if (lower === 'google') return 'Google'
  if (lower === 'github') return 'Github'
  return trimmed
}

function isEnterpriseProvider(provider?: string): boolean {
  return provider?.trim().toLowerCase() === 'enterprise'
}

function isExternalIdpProvider(provider?: string): boolean {
  const lower = provider?.trim().toLowerCase()
  return lower === 'externalidp' || lower === 'external-idp' || lower === 'external_idp' || lower === 'external idp'
}

function normalizeAuthMethod(authMethod?: string): 'social' | 'idc' | 'external_idp' | undefined {
  const lower = authMethod?.trim().toLowerCase()
  if (!lower) return undefined
  if (lower === 'external_idp' || lower === 'external-idp' || lower === 'externalidp') return 'external_idp'
  if (lower === 'idc' || lower === 'builder-id' || lower === 'iam') return 'idc'
  if (lower === 'social') return 'social'
  return undefined
}

function normalizeScopes(scopes?: string | string[]): string | undefined {
  if (Array.isArray(scopes)) {
    const joined = scopes.flatMap(scope => scope.split(/\s+/)).map(scope => scope.trim()).filter(Boolean).join(' ')
    return joined || undefined
  }
  const joined = scopes?.split(/\s+/).map(scope => scope.trim()).filter(Boolean).join(' ')
  return joined || undefined
}

function resolveKamRegions(
  cred: KamAccount['credentials'],
  enterprise: boolean
): { region: string | undefined; authRegion: string | undefined; apiRegion: string | undefined } {
  const region = cred.region?.trim() || undefined
  const explicitAuthRegion = cred.authRegion?.trim() || undefined
  const explicitApiRegion = cred.apiRegion?.trim() || undefined

  return {
    region,
    authRegion: explicitAuthRegion || (enterprise ? KAM_DEFAULT_AUTH_REGION : region),
    apiRegion: explicitApiRegion || region,
  }
}


// 兼容 KAM 1.8.3 新版平铺格式，统一转换为旧格式（credentials 嵌套结构）
function normalizeKamAccount(item: unknown): unknown {
  if (typeof item !== 'object' || item === null) return item
  const obj = item as Record<string, unknown>
  // 新格式：refreshToken 直接在账号对象上，无 credentials 嵌套
  if (typeof obj.refreshToken === 'string' && typeof obj.credentials === 'undefined') {
    const email = typeof obj.email === 'string' ? obj.email : undefined
    const userId =
      typeof obj.userId === 'string' || obj.userId === null ? (obj.userId as string | null) : undefined
    const provider = getString(obj.provider)
    const nickname =
      typeof obj.nickname === 'string'
        ? obj.nickname
        : typeof obj.label === 'string'
          ? (obj.label as string)
          : undefined
    const profileArn = typeof obj.profileArn === 'string' ? obj.profileArn : undefined
    const accountType = getString(obj.accountType)
    const sourceSupplierId = getAliasedString(obj, 'sourceSupplierId', 'source_supplier_id')
    const sourceSupplierName = getAliasedString(obj, 'sourceSupplierName', 'source_supplier_name')
    const sourceBatch = getAliasedString(obj, 'sourceBatch', 'source_batch')
    const availableModelIds = Array.isArray(obj.availableModelIds)
      ? obj.availableModelIds.filter((value): value is string => typeof value === 'string')
      : undefined
    const availableModels = Array.isArray(obj.availableModels) ? obj.availableModels : undefined
    const models = Array.isArray(obj.models) ? obj.models : undefined
    const availableModelsCache =
      typeof obj.availableModelsCache === 'object' && obj.availableModelsCache !== null
        ? obj.availableModelsCache as KamAccount['availableModelsCache']
        : undefined
    const maxConcurrency = getNumber(obj.maxConcurrency)
    const rateLimitCooldownEnabled = getBoolean(obj.rateLimitCooldownEnabled)
    const rateLimitBucketCapacity = getNumber(obj.rateLimitBucketCapacity)
    const rateLimitRefillPerSecond = getNumber(obj.rateLimitRefillPerSecond)
    const priority = getNumber(obj.priority)
    const status = getString(obj.status)
    const machineId = getString(obj.machineId)
    const clientId = getString(obj.clientId)
    const clientSecret = getString(obj.clientSecret)
    const region = getString(obj.region)
    const authRegion = getString(obj.authRegion)
    const apiRegion = getString(obj.apiRegion)
    const authMethod = getString(obj.authMethod)
    const tokenEndpoint = getString(obj.tokenEndpoint)
    const issuerUrl = getString(obj.issuerUrl)
    const scopes = typeof obj.scopes === 'string' || Array.isArray(obj.scopes)
      ? obj.scopes as string | string[]
      : undefined
    const audience = getString(obj.audience)
    const startUrl = getString(obj.startUrl)
    const proxyId = getString(obj.proxyId)
    const proxyUrl = getString(obj.proxyUrl)
    const proxyUsername = getString(obj.proxyUsername)
    const proxyPassword = getString(obj.proxyPassword)

    return {
      email,
      userId,
      provider,
      nickname,
      profileArn,
      accountType,
      sourceSupplierId,
      sourceSupplierName,
      sourceBatch,
      availableModelIds,
      availableModels,
      models,
      availableModelsCache,
      maxConcurrency,
      rateLimitCooldownEnabled,
      rateLimitBucketCapacity,
      rateLimitRefillPerSecond,
      status,
      machineId,
      credentials: {
        refreshToken: obj.refreshToken,
        clientId,
        clientSecret,
        region,
        authRegion,
        apiRegion,
        profileArn,
        authMethod,
        tokenEndpoint,
        issuerUrl,
        scopes,
        audience,
        startUrl,
        sourceSupplierId,
        sourceSupplierName,
        sourceBatch,
        priority,
        rateLimitCooldownEnabled,
        proxyId,
        proxyUrl,
        proxyUsername,
        proxyPassword,
      },
    }
  }
  return item
}

// 校验元素是否为有效的 KAM 账号结构
function isValidKamAccount(item: unknown): item is KamAccount {
  if (typeof item !== 'object' || item === null) return false
  const obj = item as Record<string, unknown>
  if (typeof obj.credentials !== 'object' || obj.credentials === null) return false
  const cred = obj.credentials as Record<string, unknown>
  return typeof cred.refreshToken === 'string' && cred.refreshToken.trim().length > 0
}

// 解析 KAM 导出 JSON，支持单账号和多账号格式
function parseKamJson(raw: string): KamAccount[] {
  const parsed = JSON.parse(raw)

  let rawItems: unknown[]

  // 标准 KAM 导出格式：{ version, accounts: [...] }
  if (parsed.accounts && Array.isArray(parsed.accounts)) {
    rawItems = parsed.accounts
  }
  // 直接数组（含 KAM 1.8.3 新版平铺格式）
  else if (Array.isArray(parsed)) {
    rawItems = parsed
  }
  // 单个账号对象（旧格式，有 credentials 字段）
  else if (parsed.credentials && typeof parsed.credentials === 'object') {
    rawItems = [parsed]
  }
  // 单个账号对象（新格式，refreshToken 平铺）
  else if (typeof parsed.refreshToken === 'string') {
    rawItems = [parsed]
  }
  else {
    throw new Error('无法识别的 KAM JSON 格式')
  }

  // 兼容新格式：将平铺账号统一转换为 credentials 嵌套结构
  const normalizedItems = rawItems.map(normalizeKamAccount)
  const validAccounts = normalizedItems.filter(isValidKamAccount)

  if (rawItems.length > 0 && validAccounts.length === 0) {
    throw new Error(`共 ${rawItems.length} 条记录，但均缺少有效的 credentials.refreshToken`)
  }

  if (validAccounts.length < rawItems.length) {
    const skipped = rawItems.length - validAccounts.length
    console.warn(`KAM 导入：跳过 ${skipped} 条缺少有效 credentials.refreshToken 的记录`)
  }

  return validAccounts
}

export function KamImportDialog({ open, onOpenChange }: KamImportDialogProps) {
  const jsonInputRef = useRef<HTMLTextAreaElement>(null)
  const [jsonInput, setJsonInput] = useState('')
  const [importing, setImporting] = useState(false)
  const [skipErrorAccounts, setSkipErrorAccounts] = useState(true)
  const [progress, setProgress] = useState({ current: 0, total: 0 })
  const [currentProcessing, setCurrentProcessing] = useState<string>('')
  const [results, setResults] = useState<VerificationResult[]>([])
  const [defaultPriority, setDefaultPriority] = useState('0')
  const [defaultMaxConcurrency, setDefaultMaxConcurrency] = useState('')
  const [defaultRateLimitCooldownMode, setDefaultRateLimitCooldownMode] =
    useState<RateLimitCooldownMode>('global')
  const [defaultSourceSupplierName, setDefaultSourceSupplierName] = useState('')
  const [defaultSourceBatch, setDefaultSourceBatch] = useState(() => formatDefaultSourceBatch())
  const [defaultProxyMode, setDefaultProxyMode] = useState<CredentialProxyMode>('auto')
  const [defaultProxyId, setDefaultProxyId] = useState('')
  const [defaultProxyUrl, setDefaultProxyUrl] = useState('')
  const [defaultProxyUsername, setDefaultProxyUsername] = useState('')
  const [defaultProxyPassword, setDefaultProxyPassword] = useState('')
  const [autoEnableOverage, setAutoEnableOverage] = useState(false)

  const { data: existingCredentials } = useCredentials()
  const { data: loadBalancingData } = useLoadBalancingMode()
  const { mutateAsync: addCredential } = useAddCredential()
  const { mutateAsync: deleteCredential } = useDeleteCredential()
  const proxyPoolOptions =
    loadBalancingData?.proxyPool?.proxies.filter((proxy) => proxy.enabled) ?? []
  const proxyPoolEnabled = loadBalancingData?.proxyPool?.enabled ?? false
  const proxyRequireProxy = loadBalancingData?.proxyPool?.requireProxy ?? false
  const sourceSupplierSuggestions = useMemo(
    () => collectSourceSupplierSuggestions(existingCredentials?.credentials),
    [existingCredentials?.credentials]
  )

  const rollbackCredential = async (id: number): Promise<{ success: boolean; error?: string }> => {
    try {
      await setCredentialDisabled(id, true)
    } catch (error) {
      return { success: false, error: `禁用失败: ${extractErrorMessage(error)}` }
    }
    try {
      await deleteCredential(id)
      return { success: true }
    } catch (error) {
      return { success: false, error: `删除失败: ${extractErrorMessage(error)}` }
    }
  }

  const resetImportDraft = () => {
    setJsonInput('')
    setProgress({ current: 0, total: 0 })
    setCurrentProcessing('')
    setResults([])
  }

  const resetForNextImport = () => {
    resetImportDraft()
    window.setTimeout(() => jsonInputRef.current?.focus(), 0)
  }

  const resetForm = () => {
    resetImportDraft()
    setDefaultPriority('0')
    setDefaultMaxConcurrency('')
    setDefaultRateLimitCooldownMode('global')
    setDefaultSourceSupplierName('')
    setDefaultSourceBatch(formatDefaultSourceBatch())
    setDefaultProxyMode('auto')
    setDefaultProxyId('')
    setDefaultProxyUrl('')
    setDefaultProxyUsername('')
    setDefaultProxyPassword('')
    setAutoEnableOverage(false)
  }

  const handleImport = async () => {
    // 先单独解析 JSON，给出精准的错误提示
    let validAccounts: KamAccount[]
    try {
      const accounts = parseKamJson(jsonInput)

      if (accounts.length === 0) {
        toast.error('没有可导入的账号')
        return
      }

      validAccounts = accounts.filter(a => a.credentials?.refreshToken)
      if (validAccounts.length === 0) {
        toast.error('没有包含有效 refreshToken 的账号')
        return
      }
    } catch (error) {
      toast.error('JSON 格式错误: ' + extractErrorMessage(error))
      return
    }

    const parsedDefaultPriority = Number.parseInt(defaultPriority.trim() || '0', 10)
    if (!Number.isInteger(parsedDefaultPriority) || parsedDefaultPriority < 0) {
      toast.error('默认优先级必须是非负整数')
      return
    }

    const parsedDefaultMaxConcurrency = defaultMaxConcurrency.trim()
      ? Number.parseInt(defaultMaxConcurrency.trim(), 10)
      : undefined
    if (
      parsedDefaultMaxConcurrency !== undefined &&
      (!Number.isInteger(parsedDefaultMaxConcurrency) || parsedDefaultMaxConcurrency <= 0)
    ) {
      toast.error('默认并发数必须是大于 0 的整数，留空表示不限制')
      return
    }
    if (defaultProxyMode === 'pool' && !defaultProxyId.trim()) {
      toast.error('请选择默认代理池节点')
      return
    }
    if (defaultProxyMode === 'custom' && !defaultProxyUrl.trim()) {
      toast.error('请输入默认代理 URL')
      return
    }
    if (defaultProxyMode === 'custom' && defaultProxyUrl.trim().toLowerCase() === 'direct') {
      toast.error('direct 请使用直连模式')
      return
    }
    if (defaultProxyMode === 'direct' && proxyRequireProxy) {
      toast.error('当前代理池要求新凭据必须绑定代理')
      return
    }
    const defaultSourceSupplierNameValue = defaultSourceSupplierName.trim()
    const defaultSourceBatchValue = defaultSourceBatch.trim()

    try {

      setImporting(true)
      setProgress({ current: 0, total: validAccounts.length })

      // 初始化结果，标记 error 状态的账号
      const initialResults: VerificationResult[] = validAccounts.map((account, i) => {
        const displayName = account.email || account.userId || account.nickname
        if (skipErrorAccounts && account.status === 'error') {
          return { index: i + 1, status: 'skipped' as const, email: displayName }
        }
        return { index: i + 1, status: 'pending' as const, email: displayName }
      })
      setResults(initialResults)

      // 重复检测
      const existingTokenHashes = new Set(
        existingCredentials?.credentials
          .map(c => c.refreshTokenHash)
          .filter((hash): hash is string => Boolean(hash)) || []
      )

      let successCount = 0
      let duplicateCount = 0
      let failCount = 0
      let skippedCount = 0

      for (let i = 0; i < validAccounts.length; i++) {
        const account = validAccounts[i]

        // 跳过 error 状态的账号
        if (skipErrorAccounts && account.status === 'error') {
          skippedCount++
          setProgress({ current: i + 1, total: validAccounts.length })
          continue
        }

        const cred = account.credentials
        const token = cred.refreshToken.trim()
        const tokenHash = await sha256Hex(token)

        setCurrentProcessing(`正在处理 ${account.email || account.userId || account.nickname || `账号 ${i + 1}`}`)
        setResults(prev => {
          const next = [...prev]
          next[i] = { ...next[i], status: 'checking' }
          return next
        })

        // 检查重复
        if (existingTokenHashes.has(tokenHash)) {
          duplicateCount++
          const existingCred = existingCredentials?.credentials.find(c => c.refreshTokenHash === tokenHash)
          setResults(prev => {
            const next = [...prev]
            next[i] = { ...next[i], status: 'duplicate', error: '该凭据已存在', email: existingCred?.email || existingCred?.userId || account.email || account.userId || undefined }
            return next
          })
          setProgress({ current: i + 1, total: validAccounts.length })
          continue
        }

        // 验活中
        setResults(prev => {
          const next = [...prev]
          next[i] = { ...next[i], status: 'verifying' }
          return next
        })

        let addedCredId: number | null = null

        try {
          const clientId = cred.clientId?.trim() || undefined
          const clientSecret = cred.clientSecret?.trim() || undefined
          const provider = normalizeProvider(account.provider || cred.provider)
          const tokenEndpoint =
            cred.tokenEndpoint?.trim() || account.tokenEndpoint?.trim() || undefined
          const issuerUrl = cred.issuerUrl?.trim() || account.issuerUrl?.trim() || undefined
          const scopes = normalizeScopes(cred.scopes ?? account.scopes)
          const audience = cred.audience?.trim() || account.audience?.trim() || undefined
          const externalIdp =
            isExternalIdpProvider(provider) ||
            normalizeAuthMethod(cred.authMethod) === 'external_idp' ||
            Boolean(issuerUrl)
          const authMethod =
            normalizeAuthMethod(cred.authMethod) ||
            (externalIdp ? 'external_idp' : clientId && clientSecret ? 'idc' : 'social')
          const startUrl = cred.startUrl?.trim() || undefined
          const enterprise = isEnterpriseProvider(provider)
          const { region: kamRegion, authRegion, apiRegion } = resolveKamRegions(cred, enterprise)
          const profileArn = account.profileArn?.trim() || cred.profileArn?.trim() || undefined
          const explicitProxyUrl = cred.proxyUrl?.trim() || account.proxyUrl?.trim() || undefined
          const explicitProxyId = cred.proxyId?.trim() || account.proxyId?.trim() || undefined
          const proxyUrl = explicitProxyUrl
            ? explicitProxyUrl
            : explicitProxyId
              ? undefined
              : defaultProxyMode === 'custom'
                ? defaultProxyUrl.trim()
                : defaultProxyMode === 'direct'
                  ? 'direct'
                  : undefined
          const proxyId = proxyUrl
            ? undefined
            : explicitProxyId ||
              (defaultProxyMode === 'pool' ? defaultProxyId.trim() || undefined : undefined)
          const proxyUsername = proxyUrl
            ? explicitProxyUrl
              ? cred.proxyUsername?.trim() || account.proxyUsername?.trim() || undefined
              : defaultProxyMode === 'custom'
                ? defaultProxyUsername.trim() || undefined
                : undefined
            : undefined
          const proxyPassword = proxyUrl
            ? explicitProxyUrl
              ? cred.proxyPassword?.trim() || account.proxyPassword?.trim() || undefined
              : defaultProxyMode === 'custom'
                ? defaultProxyPassword.trim() || undefined
                : undefined
            : undefined
          const availableModelIds = extractAvailableModelIds(account)
          const maxConcurrency =
            getKamDispatchNumber(account, 'maxConcurrency') ?? parsedDefaultMaxConcurrency
          const rateLimitCooldownEnabled =
            getKamDispatchBoolean(account, 'rateLimitCooldownEnabled') ??
            rateLimitCooldownValueFromMode(defaultRateLimitCooldownMode)
          const rateLimitBucketCapacity = getKamDispatchNumber(account, 'rateLimitBucketCapacity')
          const rateLimitRefillPerSecond = getKamDispatchNumber(account, 'rateLimitRefillPerSecond')
          const priority =
            getNumber(account.priority) ?? getNumber(account.credentials.priority) ?? parsedDefaultPriority
          const sourceSupplierId = getKamSourceString(
            account,
            'sourceSupplierId',
            'source_supplier_id'
          )
          const sourceSupplierName = getKamSourceString(
            account,
            'sourceSupplierName',
            'source_supplier_name'
          ) ?? (defaultSourceSupplierNameValue || undefined)
          const sourceBatch =
            getKamSourceString(account, 'sourceBatch', 'source_batch') ??
            (defaultSourceBatchValue || undefined)

          if (authMethod === 'idc' && (!clientId || !clientSecret)) {
            throw new Error('idc 模式需要同时提供 clientId 和 clientSecret')
          }
          if (authMethod === 'social' && (clientId || clientSecret || issuerUrl || tokenEndpoint)) {
            throw new Error('包含 clientId/clientSecret/issuerUrl/tokenEndpoint 的凭据必须指定 idc 或 external_idp')
          }
          if (enterprise && (!clientId || !clientSecret)) {
            throw new Error('Enterprise 账号必须包含 clientId 和 clientSecret')
          }
          if (enterprise && !startUrl) {
            throw new Error('Enterprise 账号必须包含 startUrl')
          }
          if (enterprise && !kamRegion && !authRegion && !apiRegion) {
            throw new Error('Enterprise 账号必须包含 region')
          }
          if (externalIdp && !clientId) {
            throw new Error('ExternalIdp 账号必须包含 clientId')
          }
          if (externalIdp && !issuerUrl) {
            throw new Error('ExternalIdp 账号必须包含 issuerUrl')
          }

          if (
            !Number.isInteger(priority) || priority < 0
          ) {
            throw new Error('priority 必须是非负整数')
          }
          if (
            maxConcurrency !== undefined &&
            (!Number.isInteger(maxConcurrency) || maxConcurrency <= 0)
          ) {
            throw new Error('maxConcurrency 必须是大于 0 的整数')
          }
          if (
            hasInvalidKamDispatchBoolean(account, 'rateLimitCooldownEnabled')
          ) {
            throw new Error('rateLimitCooldownEnabled 必须是布尔值')
          }
          if (
            rateLimitBucketCapacity !== undefined &&
            (!Number.isFinite(rateLimitBucketCapacity) ||
              rateLimitBucketCapacity < 0)
          ) {
            throw new Error('rateLimitBucketCapacity 必须是大于等于 0 的数字')
          }
          if (
            rateLimitRefillPerSecond !== undefined &&
            (!Number.isFinite(rateLimitRefillPerSecond) ||
              rateLimitRefillPerSecond < 0)
          ) {
            throw new Error('rateLimitRefillPerSecond 必须是大于等于 0 的数字')
          }

          const addedCred = await addCredential({
            refreshToken: token,
            email: account.email?.trim() || undefined,
            userId: account.userId?.trim() || undefined,
            authMethod,
            provider: externalIdp && !provider ? 'ExternalIdp' : provider,
            region: kamRegion,
            authRegion,
            apiRegion,
            profileArn,
            clientId,
            clientSecret,
            tokenEndpoint,
            issuerUrl,
            scopes,
            audience,
            startUrl,
            priority,
            machineId: account.machineId?.trim() || undefined,
            accountType: account.accountType?.trim() || undefined,
            sourceSupplierId,
            sourceSupplierName,
            sourceBatch,
            availableModelIds: availableModelIds.length > 0 ? availableModelIds : undefined,
            maxConcurrency,
            rateLimitCooldownEnabled,
            rateLimitBucketCapacity,
            rateLimitRefillPerSecond,
            proxyId,
            proxyUrl,
            proxyUsername,
            proxyPassword,
          })

          addedCredId = addedCred.credentialId

          await new Promise(resolve => setTimeout(resolve, 1000))

          let usage: string | undefined
          let balanceError: string | undefined
          try {
            let balance = await getCredentialBalance(addedCred.credentialId)
            let overageNote = ''
            const overageEnabled = balance.overageEnabled ?? balance.overageStatus === 'ENABLED'
            if (
              autoEnableOverage &&
              balance.overageCapability === 'OVERAGE_CAPABLE' &&
              !overageEnabled
            ) {
              balance = await setCredentialOverageStatus(addedCred.credentialId, true)
              overageNote = '，超额已开启'
            } else if (autoEnableOverage && balance.overageCapability !== 'OVERAGE_CAPABLE') {
              overageNote = '，不支持超额'
            }
            usage = `${balance.currentUsage}/${balance.effectiveUsageLimit ?? balance.usageLimit}${overageNote}`
          } catch (error) {
            balanceError = `获取额度失败: ${extractErrorMessage(error)}`
          }

          successCount++
          existingTokenHashes.add(tokenHash)
          setCurrentProcessing(
            balanceError
              ? `导入成功，额度获取失败: ${addedCred.email || addedCred.userId || account.email || account.userId || `账号 ${i + 1}`}`
              : `验活成功: ${addedCred.email || addedCred.userId || account.email || account.userId || `账号 ${i + 1}`}`,
          )
          setResults(prev => {
            const next = [...prev]
            next[i] = {
              ...next[i],
              status: 'verified',
              usage,
              error: balanceError,
              email: addedCred.email || addedCred.userId || account.email || account.userId || undefined,
              credentialId: addedCred.credentialId,
            }
            return next
          })
        } catch (error) {
          let rollbackStatus: VerificationResult['rollbackStatus'] = 'skipped'
          let rollbackError: string | undefined

          if (addedCredId) {
            const result = await rollbackCredential(addedCredId)
            if (result.success) {
              rollbackStatus = 'success'
            } else {
              rollbackStatus = 'failed'
              rollbackError = result.error
            }
          }

          failCount++
          setResults(prev => {
            const next = [...prev]
            next[i] = {
              ...next[i],
              status: 'failed',
              error: extractErrorMessage(error),
              rollbackStatus,
              rollbackError,
            }
            return next
          })
        }

        setProgress({ current: i + 1, total: validAccounts.length })
      }

      // 汇总
      const parts: string[] = []
      if (successCount > 0) parts.push(`成功 ${successCount}`)
      if (duplicateCount > 0) parts.push(`重复 ${duplicateCount}`)
      if (failCount > 0) parts.push(`失败 ${failCount}`)
      if (skippedCount > 0) parts.push(`跳过 ${skippedCount}`)

      if (failCount === 0 && duplicateCount === 0 && skippedCount === 0) {
        toast.success(`成功导入并验活 ${successCount} 个凭据`)
      } else {
        toast.info(`导入完成：${parts.join('，')}`)
      }
    } catch (error) {
      toast.error('导入失败: ' + extractErrorMessage(error))
    } finally {
      setImporting(false)
    }
  }

  const getStatusIcon = (status: VerificationResult['status']) => {
    switch (status) {
      case 'pending':
        return <div className="w-5 h-5 rounded-full border-2 border-gray-300" />
      case 'checking':
      case 'verifying':
        return <Loader2 className="w-5 h-5 animate-spin text-blue-500" />
      case 'verified':
        return <CheckCircle2 className="w-5 h-5 text-green-500" />
      case 'duplicate':
        return <AlertCircle className="w-5 h-5 text-yellow-500" />
      case 'skipped':
        return <AlertCircle className="w-5 h-5 text-gray-400" />
      case 'failed':
        return <XCircle className="w-5 h-5 text-red-500" />
    }
  }

  const getStatusText = (result: VerificationResult) => {
    switch (result.status) {
      case 'pending': return '等待中'
      case 'checking': return '检查重复...'
      case 'verifying': return '验活中...'
      case 'verified': return '验活成功'
      case 'duplicate': return '重复凭据'
      case 'skipped': return '已跳过（error 状态）'
      case 'failed':
        if (result.rollbackStatus === 'success') return '验活失败（已排除）'
        if (result.rollbackStatus === 'failed') return '验活失败（未排除）'
        return '验活失败（未创建）'
    }
  }

  // 预览解析结果
  const { previewAccounts, parseError } = useMemo(() => {
    if (!jsonInput.trim()) return { previewAccounts: [] as KamAccount[], parseError: '' }
    try {
      return { previewAccounts: parseKamJson(jsonInput), parseError: '' }
    } catch (e) {
      return { previewAccounts: [] as KamAccount[], parseError: extractErrorMessage(e) }
    }
  }, [jsonInput])

  const errorAccountCount = previewAccounts.filter(a => a.status === 'error').length

  return (
    <Dialog
      open={open}
      onOpenChange={(newOpen) => {
        if (!newOpen && importing) return
        onOpenChange(newOpen)
      }}
    >
      <DialogContent className="sm:max-w-2xl max-h-[80vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>KAM 账号导入（自动验活）</DialogTitle>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto space-y-4 py-4">
          <div className="space-y-2">
            <label className="text-sm font-medium">KAM 导出 JSON</label>
            <div className="grid gap-3 rounded-md border p-3 md:grid-cols-4">
              <div className="space-y-1.5">
                <label htmlFor="kamDefaultPriority" className="text-xs font-medium text-muted-foreground">
                  默认优先级
                </label>
                <Input
                  id="kamDefaultPriority"
                  type="number"
                  min="0"
                  value={defaultPriority}
                  onChange={(e) => setDefaultPriority(e.target.value)}
                  disabled={importing}
                />
              </div>
              <div className="space-y-1.5">
                <label htmlFor="kamDefaultMaxConcurrency" className="text-xs font-medium text-muted-foreground">
                  默认并发数
                </label>
                <Input
                  id="kamDefaultMaxConcurrency"
                  type="number"
                  min="1"
                  placeholder="不限"
                  value={defaultMaxConcurrency}
                  onChange={(e) => setDefaultMaxConcurrency(e.target.value)}
                  disabled={importing}
                />
              </div>
              <div className="space-y-1.5">
                <label htmlFor="kamDefaultRateLimitCooldown" className="text-xs font-medium text-muted-foreground">
                  429 退避
                </label>
                <select
                  id="kamDefaultRateLimitCooldown"
                  value={defaultRateLimitCooldownMode}
                  onChange={(e) =>
                    setDefaultRateLimitCooldownMode(e.target.value as RateLimitCooldownMode)
                  }
                  disabled={importing}
                  className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  <option value="global">跟随全局</option>
                  <option value="enabled">强制开启</option>
                  <option value="disabled">强制关闭</option>
                </select>
              </div>
              <div className="flex items-center justify-between gap-3 rounded-md bg-muted/30 px-3 py-2">
                <div className="text-sm font-medium">自动超额</div>
                <Switch
                  checked={autoEnableOverage}
                  onCheckedChange={(checked) => setAutoEnableOverage(Boolean(checked))}
                  disabled={importing}
                />
              </div>
            </div>
            <div className="space-y-3 rounded-md border p-3">
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2 text-sm font-medium">
                  <Tags className="h-4 w-4 text-muted-foreground" />
                  默认来源标记
                </div>
                <span className="text-xs text-muted-foreground">KAM 单账号来源字段优先</span>
              </div>
              <div className="grid gap-3 md:grid-cols-[minmax(0,1fr)_160px_auto] md:items-end">
                <div className="space-y-1.5">
                  <label htmlFor="kamDefaultSourceSupplier" className="text-xs font-medium text-muted-foreground">
                    默认供应商
                  </label>
                  <Input
                    id="kamDefaultSourceSupplier"
                    list="kam-source-supplier-options"
                    placeholder="可输入或选择"
                    value={defaultSourceSupplierName}
                    onChange={(e) => setDefaultSourceSupplierName(e.target.value)}
                    disabled={importing}
                  />
                  <datalist id="kam-source-supplier-options">
                    {sourceSupplierSuggestions.map((supplier) => (
                      <option key={supplier} value={supplier} />
                    ))}
                  </datalist>
                </div>
                <div className="space-y-1.5">
                  <label htmlFor="kamDefaultSourceBatch" className="text-xs font-medium text-muted-foreground">
                    默认批次
                  </label>
                  <Input
                    id="kamDefaultSourceBatch"
                    placeholder={formatDefaultSourceBatch()}
                    value={defaultSourceBatch}
                    onChange={(e) => setDefaultSourceBatch(e.target.value)}
                    disabled={importing}
                  />
                </div>
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  className="h-10 whitespace-nowrap"
                  onClick={() => setDefaultSourceBatch(formatDefaultSourceBatch())}
                  disabled={importing}
                >
                  当前小时
                </Button>
              </div>
            </div>
            <div className="space-y-3 rounded-md border p-3">
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2 text-sm font-medium">
                  <Network className="h-4 w-4 text-muted-foreground" />
                  默认代理策略
                </div>
                <span className="text-xs text-muted-foreground">
                  KAM 单账号代理字段优先
                </span>
              </div>
              <div className="grid gap-2 md:grid-cols-4">
                {([
                  ['auto', Shuffle, '自动均衡'],
                  ['pool', Server, '指定节点'],
                  ['custom', Link2, '自定义'],
                  ['direct', Globe2, '直连'],
                ] as const).map(([mode, Icon, label]) => (
                  <button
                    key={mode}
                    type="button"
                    onClick={() => setDefaultProxyMode(mode)}
                    disabled={
                      importing ||
                      (mode === 'pool' && (!proxyPoolEnabled || proxyPoolOptions.length === 0)) ||
                      (mode === 'direct' && proxyRequireProxy)
                    }
                    className={cn(
                      'flex h-10 items-center justify-center gap-2 rounded-md border px-3 text-sm transition-colors',
                      defaultProxyMode === mode
                        ? 'border-primary bg-primary/10 text-primary'
                        : 'border-input bg-background hover:bg-muted/60',
                      ((mode === 'pool' && (!proxyPoolEnabled || proxyPoolOptions.length === 0)) ||
                        (mode === 'direct' && proxyRequireProxy)) &&
                        'cursor-not-allowed opacity-50'
                    )}
                  >
                    <Icon className="h-4 w-4" />
                    {label}
                  </button>
                ))}
              </div>
              {defaultProxyMode === 'auto' && (
                <div className="text-xs text-muted-foreground">
                  {proxyPoolEnabled
                    ? `未指定代理的账号会自动分配到 ${proxyPoolOptions.length} 个可用代理池节点。`
                    : '代理池未启用时，未指定代理的账号会跟随全局代理配置。'}
                </div>
              )}
              {defaultProxyMode === 'pool' && (
                <select
                  value={defaultProxyId}
                  onChange={(e) => setDefaultProxyId(e.target.value)}
                  disabled={importing || proxyPoolOptions.length === 0}
                  className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  <option value="">选择默认代理池节点</option>
                  {proxyPoolOptions.map((proxy) => (
                    <option key={proxy.id} value={proxy.id}>
                      {proxyPoolEntryLabel(proxy)}
                    </option>
                  ))}
                </select>
              )}
              {defaultProxyMode === 'custom' && (
                <div className="space-y-2">
                  <Input
                    placeholder="默认代理 URL"
                    value={defaultProxyUrl}
                    onChange={(e) => setDefaultProxyUrl(e.target.value)}
                    disabled={importing}
                  />
                  <div className="grid grid-cols-2 gap-2">
                    <Input
                      placeholder="默认代理用户名"
                      value={defaultProxyUsername}
                      onChange={(e) => setDefaultProxyUsername(e.target.value)}
                      disabled={importing}
                    />
                    <Input
                      type="password"
                      placeholder="默认代理密码"
                      value={defaultProxyPassword}
                      onChange={(e) => setDefaultProxyPassword(e.target.value)}
                      disabled={importing}
                    />
                  </div>
                </div>
              )}
            </div>
            <textarea
              ref={jsonInputRef}
              placeholder={'粘贴 Kiro Account Manager 导出的 JSON\n\n支持 KAM 1.8.3+ 新版平铺格式：\n[\n  {\n    "email": "...",\n    "userId": "...",\n    "provider": "Enterprise",\n    "refreshToken": "...",\n    "clientId": "...",\n    "clientSecret": "...",\n    "region": "eu-central-1",\n    "authRegion": "us-east-1",\n    "startUrl": "https://example.awsapps.com/start",\n    "accountType": "power",\n    "sourceSupplierName": "供应商A",\n    "sourceBatch": "20260618211",\n    "maxConcurrency": 20,\n    "rateLimitCooldownEnabled": true\n  }\n]\n\n（可选的 authMethod 字段会被忽略，系统会根据 clientId/clientSecret 自动判断；未提供 authRegion 时，Enterprise 默认使用 us-east-1 进行 OIDC 刷新）\n\n也支持旧版嵌套格式：\n{\n  "version": "1.5.0",\n  "accounts": [\n    {\n      "email": "...",\n      "provider": "Enterprise",\n      "sourceSupplierName": "供应商A",\n      "sourceBatch": "20260618211",\n      "credentials": {\n        "refreshToken": "...",\n        "clientId": "...",\n        "clientSecret": "...",\n        "region": "eu-central-1",\n        "authRegion": "us-east-1",\n        "startUrl": "https://example.awsapps.com/start"\n      }\n    }\n  ]\n}'}
              value={jsonInput}
              onChange={(e) => setJsonInput(e.target.value)}
              disabled={importing}
              className="flex min-h-[200px] w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 font-mono"
            />
          </div>

          {/* 解析预览 */}
          {parseError && (
            <div className="text-sm text-red-600 dark:text-red-400">解析失败: {parseError}</div>
          )}
          {previewAccounts.length > 0 && !importing && results.length === 0 && (
            <div className="space-y-2">
              <div className="text-sm text-muted-foreground">
                识别到 {previewAccounts.length} 个账号
                {errorAccountCount > 0 && `（其中 ${errorAccountCount} 个为 error 状态）`}
              </div>
              {errorAccountCount > 0 && (
                <label className="flex items-center gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={skipErrorAccounts}
                    onChange={(e) => setSkipErrorAccounts(e.target.checked)}
                    className="rounded border-gray-300"
                  />
                  跳过 error 状态的账号
                </label>
              )}
            </div>
          )}

          {/* 导入进度和结果 */}
          {(importing || results.length > 0) && (
            <>
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span>{importing ? '导入进度' : '导入完成'}</span>
                  <span>{progress.current} / {progress.total}</span>
                </div>
                <div className="w-full bg-secondary rounded-full h-2">
                  <div
                    className="bg-primary h-2 rounded-full transition-all"
                    style={{ width: `${progress.total > 0 ? (progress.current / progress.total) * 100 : 0}%` }}
                  />
                </div>
                {importing && currentProcessing && (
                  <div className="text-xs text-muted-foreground">{currentProcessing}</div>
                )}
              </div>

              <div className="flex gap-4 text-sm">
                <span className="text-green-600 dark:text-green-400">
                  ✓ 成功: {results.filter(r => r.status === 'verified').length}
                </span>
                <span className="text-yellow-600 dark:text-yellow-400">
                  ⚠ 重复: {results.filter(r => r.status === 'duplicate').length}
                </span>
                <span className="text-red-600 dark:text-red-400">
                  ✗ 失败: {results.filter(r => r.status === 'failed').length}
                </span>
                <span className="text-gray-500">
                  ○ 跳过: {results.filter(r => r.status === 'skipped').length}
                </span>
              </div>

              <div className="border rounded-md divide-y max-h-[300px] overflow-y-auto">
                {results.map((result) => (
                  <div key={result.index} className="p-3">
                    <div className="flex items-start gap-3">
                      {getStatusIcon(result.status)}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-medium">
                            {result.email || `账号 #${result.index}`}
                          </span>
                          <span className="text-xs text-muted-foreground">
                            {getStatusText(result)}
                          </span>
                        </div>
                        {result.usage && (
                          <div className="text-xs text-muted-foreground mt-1">用量: {result.usage}</div>
                        )}
                        {result.error && (
                          <div className="text-xs text-red-600 dark:text-red-400 mt-1">{result.error}</div>
                        )}
                        {result.rollbackError && (
                          <div className="text-xs text-red-600 dark:text-red-400 mt-1">回滚失败: {result.rollbackError}</div>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </>
          )}
        </div>

        <DialogFooter className="gap-2 sm:justify-between sm:space-x-0">
          <Button
            type="button"
            variant="outline"
            onClick={resetForm}
            disabled={importing}
          >
            清空全部
          </Button>
          <div className="flex flex-col-reverse gap-2 sm:flex-row sm:justify-end">
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={importing}
            >
              {importing ? '导入中...' : results.length > 0 ? '关闭' : '取消'}
            </Button>
            {results.length > 0 ? (
              <Button
                type="button"
                onClick={resetForNextImport}
                disabled={importing}
              >
                继续导入
              </Button>
            ) : (
              <Button
                type="button"
                onClick={handleImport}
                disabled={importing || !jsonInput.trim() || previewAccounts.length === 0 || !!parseError}
              >
                开始导入并验活
              </Button>
            )}
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
