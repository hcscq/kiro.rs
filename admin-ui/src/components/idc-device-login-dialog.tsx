import { useEffect, useMemo, useRef, useState, type FormEvent } from 'react'
import {
  CheckCircle2,
  Copy,
  Download,
  ExternalLink,
  Globe2,
  Link2,
  Loader2,
  LogIn,
  Network,
  Search,
  Send,
  Server,
  Shuffle,
  Tags,
  XCircle,
} from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import {
  cancelExternalIdpLogin,
  cancelIdcDeviceLogin,
  getExternalIdpLoginStatus,
  getIdcDeviceLoginStatus,
  probeExternalIdp,
  startExternalIdpLogin,
  startIdcDeviceLogin,
  submitExternalIdpCallback,
} from '@/api/credentials'
import { useCredentials, useLoadBalancingMode } from '@/hooks/use-credentials'
import { storage } from '@/lib/storage'
import { cn, extractErrorMessage } from '@/lib/utils'
import {
  collectSourceSupplierSuggestions,
  formatDefaultSourceBatch,
} from '@/lib/source-metadata'
import {
  persistCredentialDefaultsDraft,
  readCredentialDefaultsDraft,
} from '@/lib/credential-defaults'
import { CredentialGroupPicker } from '@/components/credential-group-picker'
import { normalizeCredentialGroups } from '@/lib/credential-groups'
import type {
  CredentialProxyMode,
  ExternalIdpLoginStartResponse,
  ExternalIdpLoginStatusResponse,
  ExternalIdpProbeResponse,
  ExternalIdpProbeStatus,
  IdcDeviceLoginStartResponse,
  IdcDeviceLoginStatusResponse,
  ProxyPoolEntry,
  StartExternalIdpLoginRequest,
  StartIdcDeviceLoginRequest,
} from '@/types/api'

interface IdcDeviceLoginDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

type LoginMode = 'idc' | 'external_idp'
type LoginProvider = 'BuilderId' | 'Enterprise'
type ExternalIdpSession = ExternalIdpLoginStartResponse | ExternalIdpLoginStatusResponse
type LoginSession = IdcDeviceLoginStartResponse | IdcDeviceLoginStatusResponse | ExternalIdpSession

const DEFAULT_IDC_AUTH_REGIONS = [
  'us-east-1',
  'us-east-2',
  'us-west-2',
  'eu-central-1',
  'eu-west-1',
  'ap-northeast-1',
  'ap-southeast-1',
]
const BUILDER_ID_START_URL = 'https://view.awsapps.com/start'

function uniqueValues(values: string[]): string[] {
  const seen = new Set<string>()
  const result: string[] = []

  values.forEach((value) => {
    const normalized = value.trim()
    const key = normalized.toLowerCase()
    if (!normalized || seen.has(key)) return
    seen.add(key)
    result.push(normalized)
  })

  return result
}

function isPending(session: LoginSession | null): boolean {
  return session?.status === 'pending'
}

function displayExpiresAt(value?: string): string {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '-'
  return date.toLocaleTimeString()
}

function sessionCredentialId(session: LoginSession): number | undefined {
  return 'credentialId' in session ? session.credentialId : undefined
}

function sessionEmail(session: LoginSession): string | undefined {
  return 'email' in session ? session.email : undefined
}

function sessionUserCode(session: LoginSession): string | undefined {
  return 'userCode' in session ? session.userCode : undefined
}

function isIdcSession(
  session: LoginSession
): session is IdcDeviceLoginStartResponse | IdcDeviceLoginStatusResponse {
  return 'region' in session
}

function isExternalIdpSession(session: LoginSession): session is ExternalIdpSession {
  return 'phase' in session
}

function buildWindowsExternalIdpCallbackHelperScript(origin: string, sessionId: string): string {
  const baseUrl = origin.replace(/\/+$/, '')
  const stateEndpoint = `${baseUrl}/api/admin/auth/external-idp/callback`
  const sessionEndpoint = `${baseUrl}/api/admin/auth/external-idp/${encodeURIComponent(sessionId)}/callback`
  return `$StateEndpoint = ${JSON.stringify(stateEndpoint)}
$SessionEndpoint = ${JSON.stringify(sessionEndpoint)}
$InstallDir = Join-Path $env:LOCALAPPDATA "kiro-rs-callback"
New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
$ScriptPath = Join-Path $InstallDir "kiro-callback.ps1"
$ScriptBody = @'
param([string]$CallbackUrl)
$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($CallbackUrl)) { exit 1 }
$StateEndpoint = "__STATE_ENDPOINT__"
$SessionEndpoint = "__SESSION_ENDPOINT__"
$LogPath = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "callback.log"
try {
  [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
  Add-Type -AssemblyName System.Web
  $Body = @{ callbackUrl = $CallbackUrl } | ConvertTo-Json -Compress
  $Uri = [Uri]$CallbackUrl
  $Params = [System.Web.HttpUtility]::ParseQueryString($Uri.Query)
  $Endpoint = if ([string]::IsNullOrWhiteSpace($Params["state"])) { $SessionEndpoint } else { $StateEndpoint }
  $Response = Invoke-RestMethod -Method Post -Uri $Endpoint -ContentType "application/json" -Body $Body
  Add-Content -Path $LogPath -Value ("{0} submitted callback endpoint={1} status={2}" -f (Get-Date).ToString("s"), $Endpoint, $Response.status)
} catch {
  $Message = $_.Exception.Message
  Add-Content -Path $LogPath -Value ("{0} failed: {1}" -f (Get-Date).ToString("s"), $Message)
  try {
    $Shell = New-Object -ComObject WScript.Shell
    $PopupMessage = "kiro-rs callback failed. See " + $LogPath + [Environment]::NewLine + $Message
    $Shell.Popup($PopupMessage, 12, "kiro-rs callback", 48) | Out-Null
  } catch {}
  exit 1
}
'@
$ScriptBody = $ScriptBody.Replace("__STATE_ENDPOINT__", $StateEndpoint).Replace("__SESSION_ENDPOINT__", $SessionEndpoint)
Set-Content -Path $ScriptPath -Value $ScriptBody -Encoding UTF8
New-Item -Path "HKCU:\\Software\\Classes\\kiro" -Force | Out-Null
Set-Item -Path "HKCU:\\Software\\Classes\\kiro" -Value "URL:Kiro OAuth Callback"
New-ItemProperty -Path "HKCU:\\Software\\Classes\\kiro" -Name "URL Protocol" -Value "" -PropertyType String -Force | Out-Null
New-Item -Path "HKCU:\\Software\\Classes\\kiro\\shell\\open\\command" -Force | Out-Null
$Command = 'powershell.exe -NoProfile -ExecutionPolicy Bypass -File "' + $ScriptPath + '" "%1"'
Set-Item -Path "HKCU:\\Software\\Classes\\kiro\\shell\\open\\command" -Value $Command
Write-Host "Registered kiro:// callback helper"
`
}

function base64EncodeUtf8(value: string): string {
  const bytes = new TextEncoder().encode(value)
  let binary = ''
  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte)
  })
  return window.btoa(binary)
}

function chunkString(value: string, size: number): string[] {
  const chunks: string[] = []
  for (let index = 0; index < value.length; index += size) {
    chunks.push(value.slice(index, index + size))
  }
  return chunks
}

function buildWindowsExternalIdpCallbackHelperInstaller(origin: string, sessionId: string): string {
  const installerScript = buildWindowsExternalIdpCallbackHelperScript(origin, sessionId)
  const chunks = chunkString(base64EncodeUtf8(installerScript), 76)
    .map((chunk) => `echo ${chunk}`)
    .join('\r\n')

  return `@echo off
setlocal
set "INSTALL_DIR=%LOCALAPPDATA%\\kiro-rs-callback"
if not exist "%INSTALL_DIR%" mkdir "%INSTALL_DIR%"
set "INSTALLER=%INSTALL_DIR%\\install-kiro-rs-callback.ps1"
set "B64=%INSTALL_DIR%\\install-kiro-rs-callback.ps1.b64"
> "%B64%" (
${chunks}
)
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; $b64=(Get-Content -Raw $env:B64).Replace([string][char]13,'').Replace([string][char]10,''); [System.IO.File]::WriteAllBytes($env:INSTALLER, [Convert]::FromBase64String($b64)); & $env:INSTALLER"
if errorlevel 1 (
  echo Failed to install kiro:// callback helper.
  pause
  exit /b 1
)
echo Installed kiro:// callback helper.
pause
`
}

function downloadTextFile(filename: string, content: string): void {
  const blob = new Blob([content], { type: 'text/plain;charset=utf-8' })
  const url = window.URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  document.body.appendChild(link)
  link.click()
  link.remove()
  window.URL.revokeObjectURL(url)
}

function sessionSecondaryText(session: LoginSession): string {
  if (isIdcSession(session)) {
    return session.region
  }
  if (isExternalIdpSession(session)) {
    return session.phase
  }
  return ''
}

function sessionAuthUrl(session: LoginSession): string | undefined {
  if (isIdcSession(session)) {
    return session.verificationUriComplete || session.verificationUri
  }
  if (isExternalIdpSession(session)) {
    return session.verificationUriComplete || session.authUrl || session.verificationUri
  }
  return undefined
}

function probeStatusText(status: ExternalIdpProbeStatus): string {
  switch (status) {
    case 'ok':
      return '通过'
    case 'not-found':
      return '未发现'
    case 'failed':
      return '失败'
    default:
      return '跳过'
  }
}

function probeStatusVariant(status: ExternalIdpProbeStatus): 'success' | 'warning' | 'destructive' | 'secondary' {
  switch (status) {
    case 'ok':
      return 'success'
    case 'not-found':
      return 'warning'
    case 'failed':
      return 'destructive'
    default:
      return 'secondary'
  }
}

function boolStatusVariant(value: boolean): 'success' | 'warning' {
  return value ? 'success' : 'warning'
}

function boolStatusText(value: boolean): string {
  return value ? '支持' : '待验证'
}

function proxyPoolEntryLabel(proxy: ProxyPoolEntry): string {
  const egress = proxy.expectedEgressIp ? ` (${proxy.expectedEgressIp})` : ''
  const assigned = typeof proxy.assignedCredentials === 'number'
    ? ` · 已挂载 ${proxy.assignedCredentials} 凭据`
    : ''
  return `${proxy.id}${egress}${assigned}`
}

export function IdcDeviceLoginDialog({ open, onOpenChange }: IdcDeviceLoginDialogProps) {
  const queryClient = useQueryClient()
  const initialDefaults = useMemo(() => readCredentialDefaultsDraft(), [])
  const [mode, setMode] = useState<LoginMode>('idc')
  const [provider, setProvider] = useState<LoginProvider>('BuilderId')
  const [startUrl, setStartUrl] = useState('')
  const [region, setRegion] = useState(initialDefaults.authRegion)
  const [apiRegion, setApiRegion] = useState(initialDefaults.apiRegion)
  const [profileArn, setProfileArn] = useState(initialDefaults.profileArn)
  const [priority, setPriority] = useState(initialDefaults.priority)
  const [maxConcurrency, setMaxConcurrency] = useState(initialDefaults.maxConcurrency)
  const [machineId, setMachineId] = useState(initialDefaults.machineId)
  const [accountType, setAccountType] = useState(initialDefaults.accountType)
  const [sourceSupplierName, setSourceSupplierName] = useState(initialDefaults.sourceSupplierName)
  const [sourceSupplierId, setSourceSupplierId] = useState(initialDefaults.sourceSupplierId)
  const [sourceBatch, setSourceBatch] = useState(initialDefaults.sourceBatch)
  const [credentialGroups, setCredentialGroups] = useState(initialDefaults.credentialGroups)
  const [proxyMode, setProxyMode] = useState<CredentialProxyMode>(initialDefaults.proxyMode)
  const [proxyId, setProxyId] = useState(initialDefaults.proxyId)
  const [proxyUrl, setProxyUrl] = useState(initialDefaults.proxyUrl)
  const [proxyUsername, setProxyUsername] = useState(initialDefaults.proxyUsername)
  const [proxyPassword, setProxyPassword] = useState('')
  const [recentAuthRegions, setRecentAuthRegions] = useState<string[]>(() =>
    storage.getRecentIdcAuthRegions()
  )
  const [recentStartUrls, setRecentStartUrls] = useState<string[]>(() =>
    storage.getRecentIdcStartUrls()
  )
  const [externalWorkEmail, setExternalWorkEmail] = useState('')
  const [externalDomainName, setExternalDomainName] = useState('')
  const [externalIssuerUrl, setExternalIssuerUrl] = useState('')
  const [externalClientId, setExternalClientId] = useState('')
  const [externalScopes, setExternalScopes] = useState('')
  const [externalAudience, setExternalAudience] = useState('')
  const [externalProbe, setExternalProbe] = useState<ExternalIdpProbeResponse | null>(null)
  const [externalProbing, setExternalProbing] = useState(false)
  const [externalCallbackInput, setExternalCallbackInput] = useState('')
  const [submittingCallback, setSubmittingCallback] = useState(false)
  const [session, setSession] = useState<LoginSession | null>(null)
  const [sessionMode, setSessionMode] = useState<LoginMode>('idc')
  const [starting, setStarting] = useState(false)
  const [polling, setPolling] = useState(false)
  const pollInFlightRef = useRef(false)
  const completedSessionRef = useRef<string | null>(null)
  const { data: existingCredentials } = useCredentials()
  const { data: loadBalancingData } = useLoadBalancingMode()
  const sourceSupplierSuggestions = useMemo(
    () => collectSourceSupplierSuggestions(existingCredentials?.credentials),
    [existingCredentials?.credentials]
  )
  const proxyPoolOptions =
    loadBalancingData?.proxyPool?.proxies.filter((proxy) => proxy.enabled) ?? []
  const proxyPoolEnabled = loadBalancingData?.proxyPool?.enabled ?? false
  const proxyRequireProxy = loadBalancingData?.proxyPool?.requireProxy ?? false

  const reset = () => {
    const defaults = readCredentialDefaultsDraft()
    setMode('idc')
    setProvider('BuilderId')
    setStartUrl('')
    setRegion(defaults.authRegion)
    setApiRegion(defaults.apiRegion)
    setProfileArn(defaults.profileArn)
    setPriority(defaults.priority)
    setMaxConcurrency(defaults.maxConcurrency)
    setMachineId(defaults.machineId)
    setAccountType(defaults.accountType)
    setSourceSupplierName(defaults.sourceSupplierName)
    setSourceSupplierId(defaults.sourceSupplierId)
    setSourceBatch(defaults.sourceBatch)
    setCredentialGroups(defaults.credentialGroups)
    setProxyMode(defaults.proxyMode)
    setProxyId(defaults.proxyId)
    setProxyUrl(defaults.proxyUrl)
    setProxyUsername(defaults.proxyUsername)
    setProxyPassword('')
    setExternalWorkEmail('')
    setExternalDomainName('')
    setExternalIssuerUrl('')
    setExternalClientId('')
    setExternalScopes('')
    setExternalAudience('')
    setExternalProbe(null)
    setExternalProbing(false)
    setExternalCallbackInput('')
    setSubmittingCallback(false)
    setSession(null)
    setSessionMode('idc')
    setStarting(false)
    setPolling(false)
    pollInFlightRef.current = false
    completedSessionRef.current = null
  }

  const refreshRecentIdcLoginOptions = () => {
    setRecentAuthRegions(storage.getRecentIdcAuthRegions())
    setRecentStartUrls(storage.getRecentIdcStartUrls())
  }

  const handleStartUrlChange = (value: string) => {
    setStartUrl(value)
    const authRegion = storage.getRecentIdcAuthRegionForStartUrl(value)
    if (authRegion) {
      setRegion(authRegion)
    }
  }

  const buildProxyPayload = (label = '登录'):
    | Pick<StartIdcDeviceLoginRequest, 'proxyId' | 'proxyUrl' | 'proxyUsername' | 'proxyPassword'>
    | null => {
    if (proxyMode === 'pool' && !proxyId.trim()) {
      toast.error(`${label}需要选择代理池节点`)
      return null
    }
    if (proxyMode === 'custom') {
      const url = proxyUrl.trim()
      if (!url) {
        toast.error(`${label}需要填写代理 URL`)
        return null
      }
      if (url.toLowerCase() === 'direct') {
        toast.error('direct 请使用直连模式')
        return null
      }
    }
    if (proxyMode === 'direct' && proxyRequireProxy) {
      toast.error('当前代理池要求新凭据必须绑定代理')
      return null
    }

    return {
      proxyId: proxyMode === 'pool' ? proxyId.trim() || undefined : undefined,
      proxyUrl:
        proxyMode === 'custom'
          ? proxyUrl.trim() || undefined
          : proxyMode === 'direct'
            ? 'direct'
            : undefined,
      proxyUsername: proxyMode === 'custom' ? proxyUsername.trim() || undefined : undefined,
      proxyPassword: proxyMode === 'custom' ? proxyPassword.trim() || undefined : undefined,
    }
  }

  const buildCredentialOptionsPayload = (label = '登录'):
    | Partial<StartIdcDeviceLoginRequest & StartExternalIdpLoginRequest>
    | null => {
    const parsedPriority = priority.trim() ? Number.parseInt(priority, 10) : 0
    if (!Number.isInteger(parsedPriority) || parsedPriority < 0) {
      toast.error('优先级必须是大于等于 0 的整数')
      return null
    }

    const parsedMaxConcurrency = maxConcurrency.trim()
      ? Number.parseInt(maxConcurrency, 10)
      : undefined
    if (
      parsedMaxConcurrency !== undefined &&
      (!Number.isInteger(parsedMaxConcurrency) || parsedMaxConcurrency <= 0)
    ) {
      toast.error('并发上限必须是大于 0 的整数，留空表示不限制')
      return null
    }

    const proxyPayload = buildProxyPayload(label)
    if (!proxyPayload) return null
    const normalizedCredentialGroups = normalizeCredentialGroups(credentialGroups)

    persistCredentialDefaultsDraft({
      priority: String(parsedPriority),
      maxConcurrency: maxConcurrency.trim(),
      sourceSupplierName: sourceSupplierName.trim(),
      sourceSupplierId: sourceSupplierId.trim(),
      sourceBatch: sourceBatch.trim(),
      credentialGroups: credentialGroups.trim(),
      accountType: accountType.trim(),
      authRegion: region.trim(),
      apiRegion: apiRegion.trim(),
      profileArn: profileArn.trim(),
      machineId: machineId.trim(),
      proxyMode,
      proxyId: proxyId.trim(),
      proxyUrl: proxyUrl.trim(),
      proxyUsername: proxyUsername.trim(),
    })

    return {
      authRegion: region.trim() || undefined,
      apiRegion: apiRegion.trim() || undefined,
      profileArn: profileArn.trim() || undefined,
      priority: parsedPriority,
      maxConcurrency: parsedMaxConcurrency,
      machineId: machineId.trim() || undefined,
      accountType: accountType.trim() || undefined,
      credentialGroups: normalizedCredentialGroups.length ? normalizedCredentialGroups : undefined,
      sourceSupplierId: sourceSupplierId.trim() || undefined,
      sourceSupplierName: sourceSupplierName.trim() || undefined,
      sourceBatch: sourceBatch.trim() || undefined,
      ...proxyPayload,
    }
  }

  const recordCompletedIdcLogin = (
    response: IdcDeviceLoginStartResponse | IdcDeviceLoginStatusResponse
  ) => {
    if (response.status !== 'completed') return

    storage.addRecentIdcAuthRegion(response.region)
    if (
      response.provider.toLowerCase() === 'enterprise' &&
      response.startUrl.trim() &&
      response.startUrl.trim().replace(/\/+$/, '') !== BUILDER_ID_START_URL
    ) {
      storage.addRecentIdcLoginPair({
        startUrl: response.startUrl,
        authRegion: response.region,
      })
    }
    refreshRecentIdcLoginOptions()
  }

  const handleDialogOpenChange = async (nextOpen: boolean) => {
    if (!nextOpen && isPending(session)) {
      try {
        if (sessionMode === 'external_idp') {
          await cancelExternalIdpLogin(session!.sessionId)
        } else {
          await cancelIdcDeviceLogin(session!.sessionId)
        }
      } catch {
        // 关闭时取消失败不阻塞 UI
      }
    }
    onOpenChange(nextOpen)
    if (!nextOpen) {
      reset()
    }
  }

  const handleStart = async (event: FormEvent) => {
    event.preventDefault()

    const trimmedStartUrl = startUrl.trim()
    if (provider === 'Enterprise' && !trimmedStartUrl) {
      toast.error('Enterprise 需要填写 Start URL')
      return
    }

    const credentialOptions = buildCredentialOptionsPayload('IdC 登录')
    if (!credentialOptions) return

    const payload: StartIdcDeviceLoginRequest = {
      provider,
      startUrl: trimmedStartUrl || undefined,
      ...credentialOptions,
    }

    setStarting(true)
    try {
      const response = await startIdcDeviceLogin(payload)
      setSessionMode('idc')
      setSession(response)
      toast.success('登录已开始')
    } catch (error) {
      toast.error(`启动登录失败: ${extractErrorMessage(error)}`)
    } finally {
      setStarting(false)
    }
  }

  const handleExternalProbe = async (event: FormEvent) => {
    event.preventDefault()

    const workEmail = externalWorkEmail.trim()
    const domainName = externalDomainName.trim()
    const issuerUrl = externalIssuerUrl.trim()
    if (!workEmail && !domainName && !issuerUrl) {
      toast.error('需要填写工作邮箱、域名或 Issuer URL')
      return
    }

    setExternalProbing(true)
    setExternalProbe(null)
    try {
      const proxyPayload = buildProxyPayload('External IdP 探测')
      if (!proxyPayload) return
      const response = await probeExternalIdp({
        workEmail: workEmail || undefined,
        domainName: domainName || undefined,
        issuerUrl: issuerUrl || undefined,
        clientId: externalClientId.trim() || undefined,
        scopes: externalScopes.trim() || undefined,
        audience: externalAudience.trim() || undefined,
        probeOidc: true,
        ...proxyPayload,
      })
      setExternalProbe(response)
      if (response.issuerUrl && !issuerUrl) {
        setExternalIssuerUrl(response.issuerUrl)
      }
      if (response.clientId && !externalClientId.trim()) {
        setExternalClientId(response.clientId)
      }
      if (response.scopes && response.scopes.length > 0 && !externalScopes.trim()) {
        setExternalScopes(response.scopes.join(' '))
      }
      if (response.audience && !externalAudience.trim()) {
        setExternalAudience(response.audience)
      }
      if (response.kiroMetadataStatus === 'failed' || response.oidcDiscoveryStatus === 'failed') {
        toast.warning('探测完成，部分项目需要继续校准')
      } else {
        toast.success('探测完成')
      }
    } catch (error) {
      toast.error(`探测失败: ${extractErrorMessage(error)}`)
    } finally {
      setExternalProbing(false)
    }
  }

  const handleExternalStart = async () => {
    const workEmail = externalWorkEmail.trim()
    const domainName = externalDomainName.trim()
    const issuerUrl = externalIssuerUrl.trim()
    const clientId = externalClientId.trim()
    const scopes = externalScopes.trim()

    if (!workEmail && !domainName && !issuerUrl) {
      toast.error('需要填写工作邮箱、域名或 Issuer URL')
      return
    }

    const credentialOptions = buildCredentialOptionsPayload('External IdP 登录')
    if (!credentialOptions) return

    const payload: StartExternalIdpLoginRequest = {
      workEmail: workEmail || undefined,
      domainName: domainName || undefined,
      issuerUrl: issuerUrl || undefined,
      clientId: clientId || undefined,
      scopes: scopes || undefined,
      audience: externalAudience.trim() || undefined,
      loginHint: workEmail || undefined,
      flow: 'kiro-pkce',
      callbackBaseUrl: window.location.origin,
      ...credentialOptions,
    }

    setStarting(true)
    try {
      const response = await startExternalIdpLogin(payload)
      setSessionMode('external_idp')
      setSession(response)
      const popup = window.open(response.authUrl, '_blank', 'noreferrer')
      if (!popup) {
        toast.warning('登录页面被浏览器拦截，请手动打开')
      } else {
        toast.success('登录已开始')
      }
    } catch (error) {
      toast.error(`启动 External IdP 登录失败: ${extractErrorMessage(error)}`)
    } finally {
      setStarting(false)
    }
  }

  const pollStatus = async () => {
    if (!session || session.status !== 'pending' || pollInFlightRef.current) {
      return
    }

    pollInFlightRef.current = true
    setPolling(true)
    try {
      const response =
        sessionMode === 'external_idp'
          ? await getExternalIdpLoginStatus(session.sessionId)
          : await getIdcDeviceLoginStatus(session.sessionId)
      setSession(response)

      if (response.status === 'completed' && completedSessionRef.current !== response.sessionId) {
        completedSessionRef.current = response.sessionId
        if (isIdcSession(response)) {
          recordCompletedIdcLogin(response)
        }
        toast.success(response.message || '登录完成')
        queryClient.invalidateQueries({ queryKey: ['credentials'] })
        queryClient.invalidateQueries({ queryKey: ['loadBalancingMode'] })
      } else if (response.status === 'failed') {
        toast.error(response.message || '登录失败')
      } else if (response.status === 'expired') {
        toast.error(response.message || '授权码已过期')
      }
    } catch (error) {
      toast.error(`查询登录状态失败: ${extractErrorMessage(error)}`)
    } finally {
      pollInFlightRef.current = false
      setPolling(false)
    }
  }

  const handleExternalCallbackSubmit = async (event: FormEvent) => {
    event.preventDefault()
    if (!session || sessionMode !== 'external_idp' || !isExternalIdpSession(session)) {
      return
    }

    const value = externalCallbackInput.trim()
    if (!value) {
      toast.error('需要填写回调 URL 或授权码')
      return
    }

    setSubmittingCallback(true)
    try {
      const response = await submitExternalIdpCallback(
        session.sessionId,
        value.includes('://') || value.includes('=') || value.startsWith('?')
          ? { callbackUrl: value }
          : { code: value }
      )
      setSession(response)
      if (response.status === 'completed' && completedSessionRef.current !== response.sessionId) {
        completedSessionRef.current = response.sessionId
        setExternalCallbackInput('')
        toast.success(response.message || '登录完成')
        queryClient.invalidateQueries({ queryKey: ['credentials'] })
        queryClient.invalidateQueries({ queryKey: ['loadBalancingMode'] })
      } else if (response.status === 'failed') {
        toast.error(response.message || '登录失败')
      } else if (response.status === 'expired') {
        toast.error(response.message || '授权码已过期')
      } else {
        toast.success(response.message || '回调已提交')
      }
    } catch (error) {
      toast.error(`提交回调失败: ${extractErrorMessage(error)}`)
    } finally {
      setSubmittingCallback(false)
    }
  }

  useEffect(() => {
    if (open) {
      refreshRecentIdcLoginOptions()
    }
  }, [open])

  useEffect(() => {
    if (!open || !session || session.status !== 'pending') {
      return
    }

    const delayMs = Math.max(session.intervalSeconds || 5, 2) * 1000
    const interval = window.setInterval(() => {
      void pollStatus()
    }, delayMs)

    return () => window.clearInterval(interval)
  }, [open, sessionMode, session?.sessionId, session?.status, session?.intervalSeconds])

  const handleCopyCode = async () => {
    const userCode = session ? sessionUserCode(session) : undefined
    if (!userCode) return
    try {
      await navigator.clipboard.writeText(userCode)
      toast.success('验证码已复制')
    } catch {
      toast.error('复制失败')
    }
  }

  const handleCopyAuthUrl = async () => {
    const url = session ? sessionAuthUrl(session) : undefined
    if (!url) return
    try {
      await navigator.clipboard.writeText(url)
      toast.success('链接已复制，可在无痕窗口打开')
    } catch {
      toast.error('复制失败')
    }
  }

  const handleDownloadExternalCallbackHelper = () => {
    if (!session || sessionMode !== 'external_idp' || !isExternalIdpSession(session)) {
      toast.error('请先开始 External IdP 登录')
      return
    }

    try {
      downloadTextFile(
        'install-kiro-rs-callback-helper.cmd',
        buildWindowsExternalIdpCallbackHelperInstaller(window.location.origin, session.sessionId)
      )
      toast.success('Windows 捕获器安装器已下载')
    } catch {
      toast.error('下载失败')
    }
  }

  const statusTone =
    session?.status === 'completed'
      ? 'text-green-600'
      : session?.status === 'failed' || session?.status === 'expired'
        ? 'text-destructive'
        : 'text-muted-foreground'
  const credentialId = session ? sessionCredentialId(session) : undefined
  const email = session ? sessionEmail(session) : undefined
  const secondaryText = session ? sessionSecondaryText(session) : ''
  const authUrl = session ? sessionAuthUrl(session) : undefined
  const userCode = session ? sessionUserCode(session) : undefined
  const externalSession = session && isExternalIdpSession(session) ? session : null
  const needsManualExternalCallback =
    sessionMode === 'external_idp' &&
    externalSession?.status === 'pending' &&
    externalSession.flow === 'kiro-pkce' &&
    externalSession.phase === 'idp-authorization'
  const authRegionOptions = uniqueValues([...recentAuthRegions, ...DEFAULT_IDC_AUTH_REGIONS])

  const resetSessionForNextLogin = () => {
    const nextMode = sessionMode
    setSession(null)
    setSessionMode(nextMode)
    setExternalCallbackInput('')
    setSubmittingCallback(false)
    setPolling(false)
    pollInFlightRef.current = false
    completedSessionRef.current = null
    setMode(nextMode)

    if (nextMode === 'external_idp') {
      setExternalWorkEmail('')
      setExternalProbe(null)
    }
  }

  const renderCredentialOptions = (disabled: boolean, idPrefix: string) => (
    <div className="space-y-3 rounded-md border p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2 text-sm font-medium">
          <Tags className="h-4 w-4 text-muted-foreground" />
          凭据参数
        </div>
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="h-8"
          onClick={() => setSourceBatch(formatDefaultSourceBatch())}
          disabled={disabled}
        >
          当前小时批次
        </Button>
      </div>

      <div className="grid gap-3 sm:grid-cols-3">
        <div className="space-y-1.5">
          <label htmlFor={`${idPrefix}-priority`} className="text-xs font-medium text-muted-foreground">
            优先级
          </label>
          <Input
            id={`${idPrefix}-priority`}
            type="number"
            min="0"
            value={priority}
            onChange={(event) => setPriority(event.target.value)}
            disabled={disabled}
          />
        </div>
        <div className="space-y-1.5">
          <label htmlFor={`${idPrefix}-source-supplier`} className="text-xs font-medium text-muted-foreground">
            供应商
          </label>
          <Input
            id={`${idPrefix}-source-supplier`}
            list={`${idPrefix}-source-supplier-options`}
            value={sourceSupplierName}
            onChange={(event) => setSourceSupplierName(event.target.value)}
            placeholder="可输入或选择"
            disabled={disabled}
          />
          <datalist id={`${idPrefix}-source-supplier-options`}>
            {sourceSupplierSuggestions.map((supplier) => (
              <option key={supplier} value={supplier} />
            ))}
          </datalist>
        </div>
        <div className="space-y-1.5">
          <label htmlFor={`${idPrefix}-source-batch`} className="text-xs font-medium text-muted-foreground">
            批次
          </label>
          <Input
            id={`${idPrefix}-source-batch`}
            value={sourceBatch}
            onChange={(event) => setSourceBatch(event.target.value)}
            placeholder={formatDefaultSourceBatch()}
            disabled={disabled}
          />
        </div>
      </div>

      <details className="rounded-md bg-muted/20 px-3 py-2">
        <summary className="cursor-pointer text-sm font-medium text-muted-foreground">
          更多凭据参数
        </summary>
        <div className="mt-3 space-y-3">
          <div className="grid gap-3 sm:grid-cols-3">
            <div className="space-y-1.5">
              <label htmlFor={`${idPrefix}-source-supplier-id`} className="text-xs font-medium text-muted-foreground">
                供应商 ID
              </label>
              <Input
                id={`${idPrefix}-source-supplier-id`}
                value={sourceSupplierId}
                onChange={(event) => setSourceSupplierId(event.target.value)}
                placeholder="可选"
                disabled={disabled}
              />
            </div>
            <div className="space-y-1.5">
              <label htmlFor={`${idPrefix}-max-concurrency`} className="text-xs font-medium text-muted-foreground">
                并发上限
              </label>
              <Input
                id={`${idPrefix}-max-concurrency`}
                type="number"
                min="1"
                value={maxConcurrency}
                onChange={(event) => setMaxConcurrency(event.target.value)}
                placeholder="不限"
                disabled={disabled}
              />
            </div>
            <div className="space-y-1.5">
              <label htmlFor={`${idPrefix}-account-type`} className="text-xs font-medium text-muted-foreground">
                账号类型
              </label>
              <Input
                id={`${idPrefix}-account-type`}
                value={accountType}
                onChange={(event) => setAccountType(event.target.value)}
                placeholder="可选"
                disabled={disabled}
              />
            </div>
            <div className="space-y-1.5 sm:col-span-3">
              <label htmlFor={`${idPrefix}-credential-groups`} className="text-xs font-medium text-muted-foreground">
                凭据分组
              </label>
              <CredentialGroupPicker
                id={`${idPrefix}-credential-groups`}
                value={credentialGroups}
                onChange={setCredentialGroups}
                disabled={disabled}
                compact
              />
            </div>
          </div>

          <div className="grid gap-3 sm:grid-cols-2">
            <div className="space-y-1.5">
              <label htmlFor={`${idPrefix}-auth-region`} className="text-xs font-medium text-muted-foreground">
                Auth Region
              </label>
              <Input
                id={`${idPrefix}-auth-region`}
                list="idc-auth-region-options"
                value={region}
                onChange={(event) => setRegion(event.target.value)}
                placeholder="us-east-1"
                disabled={disabled}
              />
            </div>
            <div className="space-y-1.5">
              <label htmlFor={`${idPrefix}-api-region`} className="text-xs font-medium text-muted-foreground">
                API Region
              </label>
              <Input
                id={`${idPrefix}-api-region`}
                value={apiRegion}
                onChange={(event) => setApiRegion(event.target.value)}
                placeholder="留空跟随全局"
                disabled={disabled}
              />
            </div>
          </div>

          <div className="grid gap-3 sm:grid-cols-2">
            <div className="space-y-1.5">
              <label htmlFor={`${idPrefix}-profile-arn`} className="text-xs font-medium text-muted-foreground">
                Profile ARN
              </label>
              <Input
                id={`${idPrefix}-profile-arn`}
                value={profileArn}
                onChange={(event) => setProfileArn(event.target.value)}
                placeholder="可选，留空自动发现"
                disabled={disabled}
              />
            </div>
            <div className="space-y-1.5">
              <label htmlFor={`${idPrefix}-machine-id`} className="text-xs font-medium text-muted-foreground">
                Machine ID
              </label>
              <Input
                id={`${idPrefix}-machine-id`}
                value={machineId}
                onChange={(event) => setMachineId(event.target.value)}
                placeholder="可选"
                disabled={disabled}
              />
            </div>
          </div>

          <div className="space-y-2 rounded-md border bg-background p-3">
            <div className="flex items-center gap-2 text-sm font-medium">
              <Network className="h-4 w-4 text-muted-foreground" />
              代理策略
            </div>
            <div className="grid gap-2 sm:grid-cols-4">
              {([
                ['auto', Shuffle, '自动'],
                ['pool', Server, '节点'],
                ['custom', Link2, '自定义'],
                ['direct', Globe2, '直连'],
              ] as const).map(([item, Icon, label]) => (
                <button
                  key={item}
                  type="button"
                  onClick={() => setProxyMode(item)}
                  disabled={
                    disabled ||
                    (item === 'pool' && (!proxyPoolEnabled || proxyPoolOptions.length === 0)) ||
                    (item === 'direct' && proxyRequireProxy)
                  }
                  className={cn(
                    'flex h-9 items-center justify-center gap-2 rounded-md border px-2 text-sm transition-colors',
                    proxyMode === item
                      ? 'border-primary bg-primary/10 text-primary'
                      : 'border-input bg-background hover:bg-muted/60',
                    ((item === 'pool' && (!proxyPoolEnabled || proxyPoolOptions.length === 0)) ||
                      (item === 'direct' && proxyRequireProxy)) &&
                      'cursor-not-allowed opacity-50'
                  )}
                >
                  <Icon className="h-4 w-4" />
                  {label}
                </button>
              ))}
            </div>
            {proxyMode === 'pool' && (
              <select
                value={proxyId}
                onChange={(event) => setProxyId(event.target.value)}
                disabled={disabled || proxyPoolOptions.length === 0}
                className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <option value="">选择代理池节点</option>
                {proxyPoolOptions.map((proxy) => (
                  <option key={proxy.id} value={proxy.id}>
                    {proxyPoolEntryLabel(proxy)}
                  </option>
                ))}
              </select>
            )}
            {proxyMode === 'custom' && (
              <div className="space-y-2">
                <Input
                  value={proxyUrl}
                  onChange={(event) => setProxyUrl(event.target.value)}
                  placeholder="http://proxy:3128 或 socks5://proxy:1080"
                  disabled={disabled}
                />
                <div className="grid gap-2 sm:grid-cols-2">
                  <Input
                    value={proxyUsername}
                    onChange={(event) => setProxyUsername(event.target.value)}
                    placeholder="代理用户名"
                    disabled={disabled}
                  />
                  <Input
                    type="password"
                    value={proxyPassword}
                    onChange={(event) => setProxyPassword(event.target.value)}
                    placeholder="代理密码"
                    disabled={disabled}
                  />
                </div>
              </div>
            )}
            {proxyRequireProxy && (
              <div className="text-xs text-amber-600">当前代理池启用了强制代理，不能选择直连。</div>
            )}
          </div>
        </div>
      </details>
    </div>
  )

  return (
    <Dialog open={open} onOpenChange={handleDialogOpenChange}>
      <DialogContent className="sm:max-w-2xl max-h-[85vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>在线登录</DialogTitle>
        </DialogHeader>

        {!session ? (
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-2">
              {([
                ['idc', 'BuilderId / Enterprise'],
                ['external_idp', 'External IdP'],
              ] as const).map(([item, label]) => (
                <button
                  key={item}
                  type="button"
                  onClick={() => setMode(item)}
                  disabled={starting || externalProbing}
                  className={cn(
                    'h-10 rounded-md border text-sm font-medium transition-colors',
                    mode === item
                      ? 'border-primary bg-primary/10 text-primary'
                      : 'border-input bg-background hover:bg-muted/60'
                  )}
                >
                  {label}
                </button>
              ))}
            </div>
            <datalist id="idc-auth-region-options">
              {authRegionOptions.map((item) => (
                <option key={item} value={item} />
              ))}
            </datalist>

            {mode === 'idc' ? (
              <form onSubmit={handleStart} className="space-y-4">
                <div className="grid grid-cols-2 gap-2">
                  {(['BuilderId', 'Enterprise'] as const).map((item) => (
                    <button
                      key={item}
                      type="button"
                      onClick={() => setProvider(item)}
                      disabled={starting}
                      className={cn(
                        'h-10 rounded-md border text-sm font-medium transition-colors',
                        provider === item
                          ? 'border-primary bg-primary/10 text-primary'
                          : 'border-input bg-background hover:bg-muted/60'
                      )}
                    >
                      {item}
                    </button>
                  ))}
                </div>

                {provider === 'Enterprise' && (
                  <div className="space-y-1.5">
                    <label htmlFor="idc-start-url" className="text-sm font-medium">
                      Start URL
                    </label>
                    <Input
                      id="idc-start-url"
                      list="idc-start-url-options"
                      value={startUrl}
                      onChange={(event) => handleStartUrlChange(event.target.value)}
                      placeholder="https://d-xxxxxxxxxx.awsapps.com/start"
                      disabled={starting}
                    />
                    <datalist id="idc-start-url-options">
                      {recentStartUrls.map((item) => (
                        <option key={item} value={item} />
                      ))}
                    </datalist>
                  </div>
                )}

                {renderCredentialOptions(starting, 'idc')}

                <DialogFooter>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => void handleDialogOpenChange(false)}
                    disabled={starting}
                  >
                    取消
                  </Button>
                  <Button type="submit" disabled={starting}>
                    {starting ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <LogIn className="h-4 w-4" />
                    )}
                    开始登录
                  </Button>
                </DialogFooter>
              </form>
            ) : (
              <form onSubmit={handleExternalProbe} className="space-y-4">
                <div className="grid gap-3 sm:grid-cols-2">
                  <div className="space-y-1.5">
                    <label htmlFor="external-work-email" className="text-sm font-medium">
                      工作邮箱
                    </label>
                    <Input
                      id="external-work-email"
                      value={externalWorkEmail}
                      onChange={(event) => setExternalWorkEmail(event.target.value)}
                      placeholder="name@example.com"
                      disabled={externalProbing}
                    />
                  </div>
                  <div className="space-y-1.5">
                    <label htmlFor="external-domain" className="text-sm font-medium">
                      域名
                    </label>
                    <Input
                      id="external-domain"
                      value={externalDomainName}
                      onChange={(event) => setExternalDomainName(event.target.value)}
                      placeholder="example.com"
                      disabled={externalProbing}
                    />
                  </div>
                </div>

                <details
                  className="rounded-md border p-3"
                  open={Boolean(externalIssuerUrl || externalClientId || externalScopes || externalAudience)}
                >
                  <summary className="cursor-pointer text-sm font-medium text-muted-foreground">
                    高级 IdP 参数
                  </summary>
                  <div className="mt-3 space-y-3">
                    <div className="space-y-1.5">
                      <label htmlFor="external-issuer" className="text-sm font-medium">
                        Issuer URL
                      </label>
                      <Input
                        id="external-issuer"
                        value={externalIssuerUrl}
                        onChange={(event) => setExternalIssuerUrl(event.target.value)}
                        placeholder="https://login.example.com/oauth2/default"
                        disabled={externalProbing}
                      />
                    </div>

                    <div className="grid gap-3 sm:grid-cols-2">
                      <div className="space-y-1.5">
                        <label htmlFor="external-client-id" className="text-sm font-medium">
                          Client ID
                        </label>
                        <Input
                          id="external-client-id"
                          value={externalClientId}
                          onChange={(event) => setExternalClientId(event.target.value)}
                          placeholder="可选"
                          disabled={externalProbing}
                        />
                      </div>
                      <div className="space-y-1.5">
                        <label htmlFor="external-audience" className="text-sm font-medium">
                          Audience
                        </label>
                        <Input
                          id="external-audience"
                          value={externalAudience}
                          onChange={(event) => setExternalAudience(event.target.value)}
                          placeholder="可选"
                          disabled={externalProbing}
                        />
                      </div>
                    </div>

                    <div className="space-y-1.5">
                      <label htmlFor="external-scopes" className="text-sm font-medium">
                        Scopes
                      </label>
                      <Input
                        id="external-scopes"
                        value={externalScopes}
                        onChange={(event) => setExternalScopes(event.target.value)}
                        placeholder="openid profile email offline_access"
                        disabled={externalProbing}
                      />
                    </div>
                  </div>
                </details>

                {renderCredentialOptions(externalProbing || starting, 'external')}

                {externalProbe && (
                  <div className="space-y-3 rounded-md border p-4">
                    <div className="flex flex-wrap items-center gap-2">
                      <Badge variant={probeStatusVariant(externalProbe.kiroMetadataStatus)}>
                        Kiro metadata {probeStatusText(externalProbe.kiroMetadataStatus)}
                      </Badge>
                      <Badge variant={probeStatusVariant(externalProbe.oidcDiscoveryStatus)}>
                        OIDC discovery {probeStatusText(externalProbe.oidcDiscoveryStatus)}
                      </Badge>
                      <Badge variant={boolStatusVariant(externalProbe.pkceS256Supported)}>
                        PKCE S256 {boolStatusText(externalProbe.pkceS256Supported)}
                      </Badge>
                      <Badge variant={boolStatusVariant(externalProbe.deviceCodeSupported)}>
                        Device code {boolStatusText(externalProbe.deviceCodeSupported)}
                      </Badge>
                      <Badge
                        variant={boolStatusVariant(
                          externalProbe.refreshWithoutClientSecretLikelySupported
                        )}
                      >
                        Device token{' '}
                        {externalProbe.refreshWithoutClientSecretLikelySupported ? '支持' : '需密钥'}
                      </Badge>
                    </div>

                    <div className="grid gap-2 text-sm">
                      <div className="flex justify-between gap-3">
                        <span className="text-muted-foreground">域名</span>
                        <span className="break-all text-right">{externalProbe.domainName}</span>
                      </div>
                      {externalProbe.issuerUrl && (
                        <div className="flex justify-between gap-3">
                          <span className="text-muted-foreground">Issuer</span>
                          <span className="break-all text-right">{externalProbe.issuerUrl}</span>
                        </div>
                      )}
                      {externalProbe.clientId && (
                        <div className="flex justify-between gap-3">
                          <span className="text-muted-foreground">Client ID</span>
                          <span className="break-all text-right">{externalProbe.clientId}</span>
                        </div>
                      )}
                      {externalProbe.scopes && externalProbe.scopes.length > 0 && (
                        <div className="flex justify-between gap-3">
                          <span className="text-muted-foreground">Scopes</span>
                          <span className="break-all text-right">
                            {externalProbe.scopes.join(' ')}
                          </span>
                        </div>
                      )}
                      {externalProbe.oidc?.authorizationEndpoint && (
                        <div className="flex justify-between gap-3">
                          <span className="text-muted-foreground">Authorize</span>
                          <span className="break-all text-right">
                            {externalProbe.oidc.authorizationEndpoint}
                          </span>
                        </div>
                      )}
                      {externalProbe.oidc?.tokenEndpoint && (
                        <div className="flex justify-between gap-3">
                          <span className="text-muted-foreground">Token</span>
                          <span className="break-all text-right">
                            {externalProbe.oidc.tokenEndpoint}
                          </span>
                        </div>
                      )}
                      {externalProbe.message && (
                        <div className="flex justify-between gap-3">
                          <span className="text-muted-foreground">状态</span>
                          <span className="break-words text-right">{externalProbe.message}</span>
                        </div>
                      )}
                    </div>

                    {externalProbe.recommendations && externalProbe.recommendations.length > 0 && (
                      <div className="space-y-1 text-sm">
                        {externalProbe.recommendations.map((item, index) => (
                          <div key={`${index}-${item}`} className="text-muted-foreground">
                            {item}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}

                <DialogFooter>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => void handleDialogOpenChange(false)}
                    disabled={externalProbing || starting}
                  >
                    取消
                  </Button>
                  <Button type="submit" variant="outline" disabled={externalProbing || starting}>
                    {externalProbing ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Search className="h-4 w-4" />
                    )}
                    探测
                  </Button>
                  <Button
                    type="button"
                    onClick={() => void handleExternalStart()}
                    disabled={externalProbing || starting}
                  >
                    {starting ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <LogIn className="h-4 w-4" />
                    )}
                    开始登录
                  </Button>
                </DialogFooter>
              </form>
            )}
          </div>
        ) : (
          <div className="space-y-4">
            <div className="rounded-md border p-4">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <div className="text-sm text-muted-foreground">{session.provider}</div>
                  <div className="text-sm font-medium">{secondaryText}</div>
                </div>
                <div className={cn('flex items-center gap-2 text-sm font-medium', statusTone)}>
                  {session.status === 'completed' ? (
                    <CheckCircle2 className="h-4 w-4" />
                  ) : session.status === 'failed' || session.status === 'expired' ? (
                    <XCircle className="h-4 w-4" />
                  ) : (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  )}
                  {session.status}
                </div>
              </div>

              {userCode && (
                <div className="mt-4 flex items-center justify-between gap-3 rounded-md bg-muted px-3 py-3">
                  <div className="font-mono text-2xl font-semibold tracking-widest">
                    {userCode}
                  </div>
                  <Button type="button" size="icon" variant="outline" onClick={handleCopyCode}>
                    <Copy className="h-4 w-4" />
                  </Button>
                </div>
              )}

              <div className="mt-3 grid gap-2 text-sm">
                <div className="flex justify-between gap-3">
                  <span className="text-muted-foreground">过期时间</span>
                  <span>{displayExpiresAt(session.expiresAt)}</span>
                </div>
                {session.message && (
                  <div className="flex justify-between gap-3">
                    <span className="text-muted-foreground">状态</span>
                    <span className="text-right">{session.message}</span>
                  </div>
                )}
                {credentialId && (
                  <div className="flex justify-between gap-3">
                    <span className="text-muted-foreground">凭据 ID</span>
                    <span>#{credentialId}</span>
                  </div>
                )}
                {email && (
                  <div className="flex justify-between gap-3">
                    <span className="text-muted-foreground">账号</span>
                    <span className="break-all text-right">{email}</span>
                  </div>
                )}
              </div>
            </div>

            {authUrl ? (
              <div className="grid gap-2 sm:grid-cols-2">
                <Button asChild>
                  <a
                    href={authUrl}
                    target="_blank"
                    rel="noreferrer"
                  >
                    <ExternalLink className="h-4 w-4" />
                    {sessionMode === 'external_idp' &&
                    isExternalIdpSession(session) &&
                    (session.flow === 'pkce' || session.flow === 'kiro-pkce')
                      ? '打开登录页面'
                      : '打开验证页面'}
                  </a>
                </Button>
                <Button type="button" variant="outline" onClick={handleCopyAuthUrl}>
                  <Copy className="h-4 w-4" />
                  {sessionMode === 'external_idp' ? '复制登录链接' : '复制验证链接'}
                </Button>
              </div>
            ) : null}

            {needsManualExternalCallback && externalSession ? (
              <form onSubmit={handleExternalCallbackSubmit} className="space-y-3 rounded-md border p-4">
                {externalSession.callbackUrl && (
                  <div className="flex justify-between gap-3 text-sm">
                    <span className="text-muted-foreground">Redirect URI</span>
                    <span className="break-all text-right">{externalSession.callbackUrl}</span>
                  </div>
                )}
                <div className="space-y-1.5">
                  <label htmlFor="external-callback" className="text-sm font-medium">
                    回调 URL 或授权码
                  </label>
                  <textarea
                    id="external-callback"
                    value={externalCallbackInput}
                    onChange={(event) => setExternalCallbackInput(event.target.value)}
                    placeholder="kiro://kiro.oauth/callback?code=...&state=..."
                    disabled={submittingCallback}
                    rows={3}
                    className="flex w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                  />
                </div>
                <div className="grid gap-2 sm:grid-cols-2">
                  <Button
                    type="button"
                    variant="outline"
                    onClick={handleDownloadExternalCallbackHelper}
                  >
                    <Download className="h-4 w-4" />
                    下载捕获器
                  </Button>
                  <Button type="submit" disabled={submittingCallback}>
                    {submittingCallback ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Send className="h-4 w-4" />
                    )}
                    提交回调
                  </Button>
                </div>
              </form>
            ) : null}

            <DialogFooter>
              {session.status === 'pending' ? (
                <>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => void handleDialogOpenChange(false)}
                    disabled={polling || submittingCallback}
                  >
                    取消登录
                  </Button>
                  <Button
                    type="button"
                    onClick={() => void pollStatus()}
                    disabled={polling || submittingCallback}
                  >
                    {polling && <Loader2 className="h-4 w-4 animate-spin" />}
                    刷新状态
                  </Button>
                </>
              ) : (
                <>
                  <Button type="button" variant="outline" onClick={resetSessionForNextLogin}>
                    继续登录
                  </Button>
                  <Button type="button" onClick={() => void handleDialogOpenChange(false)}>
                    关闭
                  </Button>
                </>
              )}
            </DialogFooter>
          </div>
        )}
      </DialogContent>
    </Dialog>
  )
}
