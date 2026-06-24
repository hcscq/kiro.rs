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
import { CredentialGroupPicker } from '@/components/credential-group-picker'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import {
  useAddCredential,
  useCredentialGroupsConfig,
  useCredentials,
  useDeleteCredential,
  useLoadBalancingMode,
} from '@/hooks/use-credentials'
import { getCredentialBalance, setCredentialDisabled, setCredentialOverageStatus } from '@/api/credentials'
import type { CredentialProxyMode, ProxyPoolEntry } from '@/types/api'
import { cn, extractErrorMessage, sha256Hex } from '@/lib/utils'
import {
  collectSourceSupplierSuggestions,
  formatDefaultSourceBatch,
  normalizedSourceString,
} from '@/lib/source-metadata'
import {
  persistCredentialDefaultsDraft,
  readCredentialDefaultsDraft,
  resetCredentialDefaultsDraft,
} from '@/lib/credential-defaults'
import { normalizeCredentialGroups } from '@/lib/credential-groups'

interface BatchImportDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

interface CredentialInput {
  refreshToken: string
  authMethod?: string
  email?: string
  userId?: string
  provider?: string
  clientId?: string
  clientSecret?: string
  tokenEndpoint?: string
  issuerUrl?: string
  scopes?: string | string[]
  audience?: string
  region?: string
  authRegion?: string
  apiRegion?: string
  profileArn?: string
  priority?: number
  machineId?: string
  startUrl?: string
  accountType?: string
  sourceSupplierId?: string
  sourceSupplierName?: string
  sourceBatch?: string
  source_supplier_id?: string
  source_supplier_name?: string
  source_batch?: string
  credentialGroups?: string[] | string
  credential_groups?: string[] | string
  availableModelIds?: string[]
  maxConcurrency?: number
  rateLimitCooldownEnabled?: boolean
  rateLimitBucketCapacity?: number
  rateLimitRefillPerSecond?: number
  proxyId?: string
  proxyUrl?: string
  proxyUsername?: string
  proxyPassword?: string
}

interface VerificationResult {
  index: number
  status: 'pending' | 'checking' | 'verifying' | 'verified' | 'duplicate' | 'failed'
  error?: string
  usage?: string
  email?: string
  credentialId?: number
  rollbackStatus?: 'success' | 'failed' | 'skipped'
  rollbackError?: string
}

type RateLimitCooldownMode = 'global' | 'enabled' | 'disabled'

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

export function BatchImportDialog({ open, onOpenChange }: BatchImportDialogProps) {
  const jsonInputRef = useRef<HTMLTextAreaElement>(null)
  const defaultDraft = useMemo(() => readCredentialDefaultsDraft(), [])
  const [jsonInput, setJsonInput] = useState('')
  const [importing, setImporting] = useState(false)
  const [progress, setProgress] = useState({ current: 0, total: 0 })
  const [currentProcessing, setCurrentProcessing] = useState<string>('')
  const [results, setResults] = useState<VerificationResult[]>([])
  const [defaultPriority, setDefaultPriority] = useState(defaultDraft.priority)
  const [defaultMaxConcurrency, setDefaultMaxConcurrency] = useState(defaultDraft.maxConcurrency)
  const [defaultRateLimitCooldownMode, setDefaultRateLimitCooldownMode] =
    useState<RateLimitCooldownMode>(defaultDraft.rateLimitCooldownMode)
  const [defaultSourceSupplierName, setDefaultSourceSupplierName] = useState(defaultDraft.sourceSupplierName)
  const [defaultSourceSupplierId, setDefaultSourceSupplierId] = useState(defaultDraft.sourceSupplierId)
  const [defaultSourceBatch, setDefaultSourceBatch] = useState(defaultDraft.sourceBatch)
  const [defaultCredentialGroups, setDefaultCredentialGroups] = useState(defaultDraft.credentialGroups)
  const [defaultProxyMode, setDefaultProxyMode] = useState<CredentialProxyMode>(defaultDraft.proxyMode)
  const [defaultProxyId, setDefaultProxyId] = useState(defaultDraft.proxyId)
  const [defaultProxyUrl, setDefaultProxyUrl] = useState(defaultDraft.proxyUrl)
  const [defaultProxyUsername, setDefaultProxyUsername] = useState(defaultDraft.proxyUsername)
  const [defaultProxyPassword, setDefaultProxyPassword] = useState('')
  const [autoEnableOverage, setAutoEnableOverage] = useState(defaultDraft.autoEnableOverage)

  const { data: existingCredentials } = useCredentials()
  const { data: loadBalancingData } = useLoadBalancingMode()
  const { data: credentialGroupsConfig } = useCredentialGroupsConfig()
  const { mutateAsync: addCredential } = useAddCredential()
  const { mutateAsync: deleteCredential } = useDeleteCredential()
  const proxyPoolOptions =
    loadBalancingData?.proxyPool?.proxies.filter((proxy) => proxy.enabled) ?? []
  const proxyPoolEnabled = loadBalancingData?.proxyPool?.enabled ?? false
  const proxyRequireProxy = loadBalancingData?.proxyPool?.requireProxy ?? false
  const enabledCredentialGroupNames = useMemo(
    () =>
      new Set(
        (credentialGroupsConfig?.groups ?? [])
          .filter((group) => group.enabled)
          .map((group) => group.name)
      ),
    [credentialGroupsConfig?.groups]
  )
  const uncoveredCredentialGroupNames = useMemo(() => {
    if (!credentialGroupsConfig || credentialGroupsConfig.legacyFullAccessKey) {
      return new Set<string>()
    }
    return new Set(
      credentialGroupsConfig.usage
        .filter((item) => item.known && item.apiKeyCount === 0)
        .map((item) => item.name)
    )
  }, [credentialGroupsConfig])
  const sourceSupplierSuggestions = useMemo(
    () => collectSourceSupplierSuggestions(existingCredentials?.credentials),
    [existingCredentials?.credentials]
  )

  const rollbackCredential = async (id: number): Promise<{ success: boolean; error?: string }> => {
    try {
      await setCredentialDisabled(id, true)
    } catch (error) {
      return {
        success: false,
        error: `禁用失败: ${extractErrorMessage(error)}`,
      }
    }

    try {
      await deleteCredential(id)
      return { success: true }
    } catch (error) {
      return {
        success: false,
        error: `删除失败: ${extractErrorMessage(error)}`,
      }
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
    const resetDraft = resetCredentialDefaultsDraft()
    resetImportDraft()
    setDefaultPriority(resetDraft.priority)
    setDefaultMaxConcurrency(resetDraft.maxConcurrency)
    setDefaultRateLimitCooldownMode(resetDraft.rateLimitCooldownMode)
    setDefaultSourceSupplierName(resetDraft.sourceSupplierName)
    setDefaultSourceSupplierId(resetDraft.sourceSupplierId)
    setDefaultSourceBatch(resetDraft.sourceBatch)
    setDefaultCredentialGroups(resetDraft.credentialGroups)
    setDefaultProxyMode(resetDraft.proxyMode)
    setDefaultProxyId(resetDraft.proxyId)
    setDefaultProxyUrl(resetDraft.proxyUrl)
    setDefaultProxyUsername(resetDraft.proxyUsername)
    setDefaultProxyPassword('')
    setAutoEnableOverage(resetDraft.autoEnableOverage)
  }

  const handleBatchImport = async () => {
    // 先单独解析 JSON，给出精准的错误提示
    let credentials: CredentialInput[]
    try {
      const parsed = JSON.parse(jsonInput)
      credentials = Array.isArray(parsed) ? parsed : [parsed]
    } catch (error) {
      toast.error('JSON 格式错误: ' + extractErrorMessage(error))
      return
    }

    if (credentials.length === 0) {
      toast.error('没有可导入的凭据')
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
    const defaultSourceSupplierIdValue = defaultSourceSupplierId.trim()
    const defaultSourceBatchValue = defaultSourceBatch.trim()
    const defaultCredentialGroupValues = normalizeCredentialGroups(defaultCredentialGroups)
    const batchCredentialGroups = new Set<string>()
    for (const cred of credentials) {
      const credentialGroupValues = normalizeCredentialGroups(
        cred.credentialGroups ?? cred.credential_groups ?? defaultCredentialGroupValues
      )
      credentialGroupValues.forEach((group) => batchCredentialGroups.add(group))
    }
    if (credentialGroupsConfig) {
      const unknownGroups = Array.from(batchCredentialGroups).filter(
        (group) => !enabledCredentialGroupNames.has(group)
      )
      if (unknownGroups.length > 0) {
        toast.error(`存在未登记或未启用的凭据分组: ${unknownGroups.join(', ')}`)
        return
      }
      const uncoveredGroups = Array.from(batchCredentialGroups).filter((group) =>
        uncoveredCredentialGroupNames.has(group)
      )
      if (uncoveredGroups.length > 0) {
        toast.warning(`以下分组当前没有 scoped API Key 覆盖: ${uncoveredGroups.join(', ')}`)
      }
    }

    try {
      persistCredentialDefaultsDraft({
        priority: String(parsedDefaultPriority),
        maxConcurrency: defaultMaxConcurrency.trim(),
        rateLimitCooldownMode: defaultRateLimitCooldownMode,
        sourceSupplierName: defaultSourceSupplierNameValue,
        sourceSupplierId: defaultSourceSupplierIdValue,
        sourceBatch: defaultSourceBatchValue,
        credentialGroups: defaultCredentialGroups.trim(),
        proxyMode: defaultProxyMode,
        proxyId: defaultProxyId.trim(),
        proxyUrl: defaultProxyUrl.trim(),
        proxyUsername: defaultProxyUsername.trim(),
        autoEnableOverage,
      })
      setImporting(true)
      setProgress({ current: 0, total: credentials.length })

      // 2. 初始化结果
      const initialResults: VerificationResult[] = credentials.map((credential, i) => ({
        index: i + 1,
        status: 'pending',
        email: credential.email?.trim() || credential.userId?.trim() || undefined,
      }))
      setResults(initialResults)

      // 3. 检测重复
      const existingTokenHashes = new Set(
        existingCredentials?.credentials
          .map(c => c.refreshTokenHash)
          .filter((hash): hash is string => Boolean(hash)) || []
      )

      let successCount = 0
      let duplicateCount = 0
      let failCount = 0
      let rollbackSuccessCount = 0
      let rollbackFailedCount = 0
      let rollbackSkippedCount = 0

      // 4. 导入并验活
      for (let i = 0; i < credentials.length; i++) {
        const cred = credentials[i]
        const credentialEmail = cred.email?.trim() || undefined
        const credentialUserId = cred.userId?.trim() || undefined
        const token = cred.refreshToken.trim()
        const tokenHash = await sha256Hex(token)

        // 更新状态为检查中
        setCurrentProcessing(
          credentialEmail
            ? `正在处理 ${credentialEmail}`
            : `正在处理凭据 ${i + 1}/${credentials.length}`
        )
        setResults(prev => {
          const newResults = [...prev]
          newResults[i] = { ...newResults[i], status: 'checking' }
          return newResults
        })

        // 检查重复
        if (existingTokenHashes.has(tokenHash)) {
          duplicateCount++
          const existingCred = existingCredentials?.credentials.find(c => c.refreshTokenHash === tokenHash)
          setResults(prev => {
            const newResults = [...prev]
            newResults[i] = {
              ...newResults[i],
              status: 'duplicate',
              error: '该凭据已存在',
              email: existingCred?.email || existingCred?.userId || credentialEmail || credentialUserId
            }
            return newResults
          })
          setProgress({ current: i + 1, total: credentials.length })
          continue
        }

        // 更新状态为验活中
        setResults(prev => {
          const newResults = [...prev]
          newResults[i] = { ...newResults[i], status: 'verifying' }
          return newResults
        })

        let addedCredId: number | null = null

        try {
          // 添加凭据
          const clientId = cred.clientId?.trim() || undefined
          const clientSecret = cred.clientSecret?.trim() || undefined
          const provider = normalizeProvider(cred.provider)
          const tokenEndpoint = cred.tokenEndpoint?.trim() || undefined
          const issuerUrl = cred.issuerUrl?.trim() || undefined
          const scopes = normalizeScopes(cred.scopes)
          const audience = cred.audience?.trim() || undefined
          const enterprise = isEnterpriseProvider(provider)
          const externalIdp =
            isExternalIdpProvider(provider) ||
            normalizeAuthMethod(cred.authMethod) === 'external_idp' ||
            Boolean(issuerUrl)
          const authMethod =
            normalizeAuthMethod(cred.authMethod) ||
            (externalIdp ? 'external_idp' : clientId && clientSecret ? 'idc' : 'social')
          const region = cred.region?.trim() || undefined
          const authRegion = cred.authRegion?.trim() || region || undefined
          const apiRegion = cred.apiRegion?.trim() || region || undefined
          const startUrl = cred.startUrl?.trim() || undefined
          const profileArn = cred.profileArn?.trim() || undefined
          const credentialGroupValues = normalizeCredentialGroups(
            cred.credentialGroups ?? cred.credential_groups ?? defaultCredentialGroupValues
          )

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
          if (enterprise && !region && !authRegion && !apiRegion) {
            throw new Error('Enterprise 账号必须包含 region')
          }
          if (externalIdp && !clientId) {
            throw new Error('ExternalIdp 账号必须包含 clientId')
          }
          if (externalIdp && !issuerUrl) {
            throw new Error('ExternalIdp 账号必须包含 issuerUrl')
          }

          if (
            cred.priority !== undefined &&
            (!Number.isInteger(cred.priority) || cred.priority < 0)
          ) {
            throw new Error('priority 必须是非负整数')
          }
          if (
            cred.maxConcurrency !== undefined &&
            (!Number.isInteger(cred.maxConcurrency) || cred.maxConcurrency <= 0)
          ) {
            throw new Error('maxConcurrency 必须是大于 0 的整数')
          }
          if (
            cred.rateLimitCooldownEnabled !== undefined &&
            typeof cred.rateLimitCooldownEnabled !== 'boolean'
          ) {
            throw new Error('rateLimitCooldownEnabled 必须是布尔值')
          }
          if (
            cred.rateLimitBucketCapacity !== undefined &&
            (!Number.isFinite(cred.rateLimitBucketCapacity) || cred.rateLimitBucketCapacity < 0)
          ) {
            throw new Error('rateLimitBucketCapacity 必须是大于等于 0 的数字')
          }
          if (
            cred.rateLimitRefillPerSecond !== undefined &&
            (!Number.isFinite(cred.rateLimitRefillPerSecond) ||
              cred.rateLimitRefillPerSecond < 0)
          ) {
            throw new Error('rateLimitRefillPerSecond 必须是大于等于 0 的数字')
          }
          const explicitProxyUrl = cred.proxyUrl?.trim()
          const explicitProxyId = cred.proxyId?.trim()
          const resolvedProxyUrl = explicitProxyUrl
            ? explicitProxyUrl
            : explicitProxyId
              ? undefined
              : defaultProxyMode === 'custom'
                ? defaultProxyUrl.trim()
                : defaultProxyMode === 'direct'
                  ? 'direct'
                  : undefined
          const resolvedProxyId = resolvedProxyUrl
            ? undefined
            : explicitProxyId ||
              (defaultProxyMode === 'pool' ? defaultProxyId.trim() || undefined : undefined)
          const resolvedProxyUsername = resolvedProxyUrl
            ? explicitProxyUrl
              ? cred.proxyUsername?.trim() || undefined
              : defaultProxyMode === 'custom'
                ? defaultProxyUsername.trim() || undefined
                : undefined
            : undefined
          const resolvedProxyPassword = resolvedProxyUrl
            ? explicitProxyUrl
              ? cred.proxyPassword?.trim() || undefined
              : defaultProxyMode === 'custom'
                ? defaultProxyPassword.trim() || undefined
                : undefined
            : undefined

          const addedCred = await addCredential({
            refreshToken: token,
            email: credentialEmail,
            userId: credentialUserId,
            authMethod,
            provider: externalIdp && !provider ? 'ExternalIdp' : provider,
            region,
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
            priority: typeof cred.priority === 'number' ? cred.priority : parsedDefaultPriority,
            machineId: cred.machineId?.trim() || undefined,
            accountType: cred.accountType?.trim() || undefined,
            sourceSupplierId:
              normalizedSourceString(cred.sourceSupplierId) ||
              normalizedSourceString(cred.source_supplier_id) ||
              defaultSourceSupplierIdValue ||
              undefined,
            sourceSupplierName:
              normalizedSourceString(cred.sourceSupplierName) ||
              normalizedSourceString(cred.source_supplier_name) ||
              defaultSourceSupplierNameValue ||
              undefined,
            sourceBatch:
              normalizedSourceString(cred.sourceBatch) ||
              normalizedSourceString(cred.source_batch) ||
              defaultSourceBatchValue ||
              undefined,
            availableModelIds: Array.isArray(cred.availableModelIds)
              ? cred.availableModelIds.filter(modelId => typeof modelId === 'string' && modelId.trim())
              : undefined,
            credentialGroups: credentialGroupValues.length ? credentialGroupValues : undefined,
            maxConcurrency:
              typeof cred.maxConcurrency === 'number'
                ? cred.maxConcurrency
                : parsedDefaultMaxConcurrency,
            rateLimitCooldownEnabled:
              typeof cred.rateLimitCooldownEnabled === 'boolean'
                ? cred.rateLimitCooldownEnabled
                : rateLimitCooldownValueFromMode(defaultRateLimitCooldownMode),
            rateLimitBucketCapacity:
              typeof cred.rateLimitBucketCapacity === 'number'
                ? cred.rateLimitBucketCapacity
                : undefined,
            rateLimitRefillPerSecond:
              typeof cred.rateLimitRefillPerSecond === 'number'
                ? cred.rateLimitRefillPerSecond
                : undefined,
            proxyId: resolvedProxyId,
            proxyUrl: resolvedProxyUrl,
            proxyUsername: resolvedProxyUsername,
            proxyPassword: resolvedProxyPassword,
          })

          addedCredId = addedCred.credentialId

          // 延迟 1 秒
          await new Promise(resolve => setTimeout(resolve, 1000))

          // 验活
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

          // 验活成功
          successCount++
          existingTokenHashes.add(tokenHash)
          setCurrentProcessing(addedCred.email || addedCred.userId || credentialEmail || credentialUserId
            ? `验活成功: ${addedCred.email || addedCred.userId || credentialEmail || credentialUserId}`
            : `验活成功: 凭据 ${i + 1}`)
          setResults(prev => {
            const newResults = [...prev]
            newResults[i] = {
              ...newResults[i],
              status: 'verified',
              usage: `${balance.currentUsage}/${balance.effectiveUsageLimit ?? balance.usageLimit}${overageNote}`,
              email: addedCred.email || addedCred.userId || credentialEmail || credentialUserId,
              credentialId: addedCred.credentialId
            }
            return newResults
          })
        } catch (error) {
          // 验活失败，尝试回滚（先禁用再删除）
          let rollbackStatus: VerificationResult['rollbackStatus'] = 'skipped'
          let rollbackError: string | undefined

          if (addedCredId) {
            const rollbackResult = await rollbackCredential(addedCredId)
            if (rollbackResult.success) {
              rollbackStatus = 'success'
              rollbackSuccessCount++
            } else {
              rollbackStatus = 'failed'
              rollbackFailedCount++
              rollbackError = rollbackResult.error
            }
          } else {
            rollbackSkippedCount++
          }

          failCount++
          setResults(prev => {
            const newResults = [...prev]
            newResults[i] = {
              ...newResults[i],
              status: 'failed',
              error: extractErrorMessage(error),
              email: credentialEmail || credentialUserId,
              rollbackStatus,
              rollbackError,
            }
            return newResults
          })
        }

        setProgress({ current: i + 1, total: credentials.length })
      }

      // 显示结果
      if (failCount === 0 && duplicateCount === 0) {
        toast.success(`成功导入并验活 ${successCount} 个凭据`)
      } else {
        const failureSummary = failCount > 0
          ? `，失败 ${failCount} 个（已排除 ${rollbackSuccessCount}，未排除 ${rollbackFailedCount}，无需排除 ${rollbackSkippedCount}）`
          : ''
        toast.info(`验活完成：成功 ${successCount} 个，重复 ${duplicateCount} 个${failureSummary}`)

        if (rollbackFailedCount > 0) {
          toast.warning(`有 ${rollbackFailedCount} 个失败凭据回滚未完成，请手动禁用并删除`)
        }
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
      case 'failed':
        return <XCircle className="w-5 h-5 text-red-500" />
    }
  }

  const getStatusText = (result: VerificationResult) => {
    switch (result.status) {
      case 'pending':
        return '等待中'
      case 'checking':
        return '检查重复...'
      case 'verifying':
        return '验活中...'
      case 'verified':
        return '验活成功'
      case 'duplicate':
        return '重复凭据'
      case 'failed':
        if (result.rollbackStatus === 'success') return '验活失败（已排除）'
        if (result.rollbackStatus === 'failed') return '验活失败（未排除）'
        return '验活失败（未创建）'
    }
  }

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
          <DialogTitle>批量导入凭据（自动验活）</DialogTitle>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto space-y-4 py-4">
          <div className="space-y-2">
            <label className="text-sm font-medium">
              JSON 格式凭据
            </label>
            <div className="grid gap-3 rounded-md border p-3 md:grid-cols-4">
              <div className="space-y-1.5">
                <label htmlFor="batchDefaultPriority" className="text-xs font-medium text-muted-foreground">
                  默认优先级
                </label>
                <Input
                  id="batchDefaultPriority"
                  type="number"
                  min="0"
                  value={defaultPriority}
                  onChange={(e) => setDefaultPriority(e.target.value)}
                  disabled={importing}
                />
              </div>
              <div className="space-y-1.5">
                <label htmlFor="batchDefaultMaxConcurrency" className="text-xs font-medium text-muted-foreground">
                  默认并发数
                </label>
                <Input
                  id="batchDefaultMaxConcurrency"
                  type="number"
                  min="1"
                  placeholder="不限"
                  value={defaultMaxConcurrency}
                  onChange={(e) => setDefaultMaxConcurrency(e.target.value)}
                  disabled={importing}
                />
              </div>
              <div className="space-y-1.5">
                <label htmlFor="batchDefaultRateLimitCooldown" className="text-xs font-medium text-muted-foreground">
                  429 退避
                </label>
                <select
                  id="batchDefaultRateLimitCooldown"
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
                  默认凭据分组
                </div>
                <span className="text-xs text-muted-foreground">JSON 单条 credentialGroups 优先</span>
              </div>
              <CredentialGroupPicker
                id="batchDefaultCredentialGroups"
                value={defaultCredentialGroups}
                onChange={setDefaultCredentialGroups}
                disabled={importing}
              />
            </div>
            <div className="space-y-3 rounded-md border p-3">
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2 text-sm font-medium">
                  <Tags className="h-4 w-4 text-muted-foreground" />
                  默认来源标记
                </div>
                <span className="text-xs text-muted-foreground">JSON 单条来源字段优先</span>
              </div>
              <div className="grid gap-3 md:grid-cols-[minmax(0,1fr)_180px_160px_auto] md:items-end">
                <div className="space-y-1.5">
                  <label htmlFor="batchDefaultSourceSupplier" className="text-xs font-medium text-muted-foreground">
                    默认供应商
                  </label>
                  <Input
                    id="batchDefaultSourceSupplier"
                    list="batch-source-supplier-options"
                    placeholder="可输入或选择"
                    value={defaultSourceSupplierName}
                    onChange={(e) => setDefaultSourceSupplierName(e.target.value)}
                    disabled={importing}
                  />
                  <datalist id="batch-source-supplier-options">
                    {sourceSupplierSuggestions.map((supplier) => (
                      <option key={supplier} value={supplier} />
                    ))}
                  </datalist>
                </div>
                <div className="space-y-1.5">
                  <label htmlFor="batchDefaultSourceSupplierId" className="text-xs font-medium text-muted-foreground">
                    默认供应商 ID
                  </label>
                  <Input
                    id="batchDefaultSourceSupplierId"
                    placeholder="可选"
                    value={defaultSourceSupplierId}
                    onChange={(e) => setDefaultSourceSupplierId(e.target.value)}
                    disabled={importing}
                  />
                </div>
                <div className="space-y-1.5">
                  <label htmlFor="batchDefaultSourceBatch" className="text-xs font-medium text-muted-foreground">
                    默认批次
                  </label>
                  <Input
                    id="batchDefaultSourceBatch"
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
                  JSON 单条代理字段优先
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
                    ? `未指定代理的凭据会自动分配到 ${proxyPoolOptions.length} 个可用代理池节点。`
                    : '代理池未启用时，未指定代理的凭据会跟随全局代理配置。'}
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
              placeholder={'粘贴 JSON 格式的凭据（支持单个对象或数组）\nEnterprise: [{"email":"user@example.com","provider":"Enterprise","refreshToken":"...","clientId":"...","clientSecret":"...","region":"us-east-1","startUrl":"https://example.awsapps.com/start","sourceSupplierName":"供应商A","sourceBatch":"20260618211"}]\nExternalIdp: [{"authMethod":"external_idp","provider":"ExternalIdp","refreshToken":"...","clientId":"...","issuerUrl":"https://login.microsoftonline.com/.../v2.0","tokenEndpoint":"...","scopes":"..."}]\n支持 region 字段自动映射为 authRegion 和 apiRegion'}
              value={jsonInput}
              onChange={(e) => setJsonInput(e.target.value)}
              disabled={importing}
              className="flex min-h-[200px] w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 font-mono"
            />
            <p className="text-xs text-muted-foreground">
              支持附带 `email`、`userId`、`provider`、`authMethod`、`startUrl`、`issuerUrl`、`tokenEndpoint`、`scopes`、`accountType`、`credentialGroups`、`sourceSupplierName`、`sourceSupplierId`、`sourceBatch`、`availableModelIds`、`maxConcurrency`、`rateLimitCooldownEnabled`、`rateLimitBucketCapacity`、`rateLimitRefillPerSecond`。
              导入时会自动验活，失败的凭据会被排除。
            </p>
          </div>

          {(importing || results.length > 0) && (
            <>
              {/* 进度条 */}
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span>{importing ? '验活进度' : '验活完成'}</span>
                  <span>{progress.current} / {progress.total}</span>
                </div>
                <div className="w-full bg-secondary rounded-full h-2">
                  <div
                    className="bg-primary h-2 rounded-full transition-all"
                    style={{ width: `${(progress.current / progress.total) * 100}%` }}
                  />
                </div>
                {importing && currentProcessing && (
                  <div className="text-xs text-muted-foreground">
                    {currentProcessing}
                  </div>
                )}
              </div>

              {/* 统计 */}
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
              </div>

              {/* 结果列表 */}
              <div className="border rounded-md divide-y max-h-[300px] overflow-y-auto">
                {results.map((result) => (
                  <div key={result.index} className="p-3">
                    <div className="flex items-start gap-3">
                      {getStatusIcon(result.status)}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-medium">
                            {result.email || `凭据 #${result.index}`}
                          </span>
                          <span className="text-xs text-muted-foreground">
                            {getStatusText(result)}
                          </span>
                        </div>
                        {result.usage && (
                          <div className="text-xs text-muted-foreground mt-1">
                            用量: {result.usage}
                          </div>
                        )}
                        {result.error && (
                          <div className="text-xs text-red-600 dark:text-red-400 mt-1">
                            {result.error}
                          </div>
                        )}
                        {result.rollbackError && (
                          <div className="text-xs text-red-600 dark:text-red-400 mt-1">
                            回滚失败: {result.rollbackError}
                          </div>
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
              {importing ? '验活中...' : results.length > 0 ? '关闭' : '取消'}
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
                onClick={handleBatchImport}
                disabled={importing || !jsonInput.trim()}
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
