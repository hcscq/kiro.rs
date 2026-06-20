import { useEffect, useRef, useState, type FormEvent } from 'react'
import { CheckCircle2, Copy, ExternalLink, Loader2, LogIn, Search, Send, XCircle } from 'lucide-react'
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
import { cn, extractErrorMessage } from '@/lib/utils'
import type {
  ExternalIdpLoginStartResponse,
  ExternalIdpLoginStatusResponse,
  ExternalIdpProbeResponse,
  ExternalIdpProbeStatus,
  IdcDeviceLoginStartResponse,
  IdcDeviceLoginStatusResponse,
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

function buildWindowsExternalIdpCallbackHelperScript(origin: string): string {
  const endpoint = `${origin.replace(/\/+$/, '')}/api/admin/auth/external-idp/callback`
  return `$Endpoint = ${JSON.stringify(endpoint)}
$InstallDir = Join-Path $env:LOCALAPPDATA "kiro-rs-callback"
New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
$ScriptPath = Join-Path $InstallDir "kiro-callback.ps1"
$ScriptBody = @'
param([string]$CallbackUrl)
$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($CallbackUrl)) { exit 1 }
$Endpoint = "__ENDPOINT__"
$LogPath = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "callback.log"
try {
  $Body = @{ callbackUrl = $CallbackUrl } | ConvertTo-Json -Compress
  Invoke-RestMethod -Method Post -Uri $Endpoint -ContentType "application/json" -Body $Body | Out-Null
  Add-Content -Path $LogPath -Value ("{0} submitted callback" -f (Get-Date).ToString("s"))
} catch {
  Add-Content -Path $LogPath -Value ("{0} {1}" -f (Get-Date).ToString("s"), $_.Exception.Message)
  throw
}
'@
$ScriptBody = $ScriptBody.Replace("__ENDPOINT__", $Endpoint)
Set-Content -Path $ScriptPath -Value $ScriptBody -Encoding UTF8
New-Item -Path "HKCU:\\Software\\Classes\\kiro" -Force | Out-Null
Set-Item -Path "HKCU:\\Software\\Classes\\kiro" -Value "URL:Kiro OAuth Callback"
New-ItemProperty -Path "HKCU:\\Software\\Classes\\kiro" -Name "URL Protocol" -Value "" -PropertyType String -Force | Out-Null
New-Item -Path "HKCU:\\Software\\Classes\\kiro\\shell\\open\\command" -Force | Out-Null
$Command = 'powershell.exe -NoProfile -ExecutionPolicy Bypass -File "' + $ScriptPath + '" "%1"'
Set-Item -Path "HKCU:\\Software\\Classes\\kiro\\shell\\open\\command" -Value $Command
Write-Host "Registered kiro:// callback helper"`
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

export function IdcDeviceLoginDialog({ open, onOpenChange }: IdcDeviceLoginDialogProps) {
  const queryClient = useQueryClient()
  const [mode, setMode] = useState<LoginMode>('idc')
  const [provider, setProvider] = useState<LoginProvider>('BuilderId')
  const [startUrl, setStartUrl] = useState('')
  const [region, setRegion] = useState('')
  const [apiRegion, setApiRegion] = useState('')
  const [priority, setPriority] = useState('0')
  const [sourceSupplierName, setSourceSupplierName] = useState('')
  const [sourceBatch, setSourceBatch] = useState('')
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

  const reset = () => {
    setMode('idc')
    setProvider('BuilderId')
    setStartUrl('')
    setRegion('')
    setApiRegion('')
    setPriority('0')
    setSourceSupplierName('')
    setSourceBatch('')
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

    const parsedPriority = priority.trim() ? Number.parseInt(priority, 10) : 0
    if (!Number.isInteger(parsedPriority) || parsedPriority < 0) {
      toast.error('优先级必须是大于等于 0 的整数')
      return
    }

    const payload: StartIdcDeviceLoginRequest = {
      provider,
      startUrl: trimmedStartUrl || undefined,
      region: region.trim() || undefined,
      apiRegion: apiRegion.trim() || undefined,
      priority: parsedPriority,
      sourceSupplierName: sourceSupplierName.trim() || undefined,
      sourceBatch: sourceBatch.trim() || undefined,
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
      const response = await probeExternalIdp({
        workEmail: workEmail || undefined,
        domainName: domainName || undefined,
        issuerUrl: issuerUrl || undefined,
        clientId: externalClientId.trim() || undefined,
        scopes: externalScopes.trim() || undefined,
        audience: externalAudience.trim() || undefined,
        probeOidc: true,
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

    const parsedPriority = priority.trim() ? Number.parseInt(priority, 10) : 0
    if (!Number.isInteger(parsedPriority) || parsedPriority < 0) {
      toast.error('优先级必须是大于等于 0 的整数')
      return
    }

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
      apiRegion: apiRegion.trim() || undefined,
      priority: parsedPriority,
      sourceSupplierName: sourceSupplierName.trim() || undefined,
      sourceBatch: sourceBatch.trim() || undefined,
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
    if (!open || !session || session.status !== 'pending') {
      return
    }

    const delayMs = Math.max(session.intervalSeconds || 5, 2) * 1000
    const timeout = window.setTimeout(() => {
      void pollStatus()
    }, delayMs)

    return () => window.clearTimeout(timeout)
  }, [open, sessionMode, session?.sessionId, session?.status, session?.intervalSeconds, session?.message])

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

  const handleCopyExternalCallbackHelper = async () => {
    try {
      await navigator.clipboard.writeText(
        buildWindowsExternalIdpCallbackHelperScript(window.location.origin)
      )
      toast.success('Windows 捕获器安装命令已复制')
    } catch {
      toast.error('复制失败')
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

  return (
    <Dialog open={open} onOpenChange={handleDialogOpenChange}>
      <DialogContent className="sm:max-w-xl">
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
                      value={startUrl}
                      onChange={(event) => setStartUrl(event.target.value)}
                      placeholder="https://d-xxxxxxxxxx.awsapps.com/start"
                      disabled={starting}
                    />
                  </div>
                )}

                <div className="grid gap-3 sm:grid-cols-2">
                  <div className="space-y-1.5">
                    <label htmlFor="idc-region" className="text-sm font-medium">
                      Auth Region
                    </label>
                    <Input
                      id="idc-region"
                      value={region}
                      onChange={(event) => setRegion(event.target.value)}
                      placeholder="us-east-1"
                      disabled={starting}
                    />
                  </div>
                  <div className="space-y-1.5">
                    <label htmlFor="idc-api-region" className="text-sm font-medium">
                      API Region
                    </label>
                    <Input
                      id="idc-api-region"
                      value={apiRegion}
                      onChange={(event) => setApiRegion(event.target.value)}
                      placeholder="留空跟随全局"
                      disabled={starting}
                    />
                  </div>
                </div>

                <div className="grid gap-3 sm:grid-cols-3">
                  <div className="space-y-1.5">
                    <label htmlFor="idc-priority" className="text-sm font-medium">
                      优先级
                    </label>
                    <Input
                      id="idc-priority"
                      value={priority}
                      onChange={(event) => setPriority(event.target.value)}
                      inputMode="numeric"
                      disabled={starting}
                    />
                  </div>
                  <div className="space-y-1.5 sm:col-span-2">
                    <label htmlFor="idc-source-name" className="text-sm font-medium">
                      供应商
                    </label>
                    <Input
                      id="idc-source-name"
                      value={sourceSupplierName}
                      onChange={(event) => setSourceSupplierName(event.target.value)}
                      placeholder="可选"
                      disabled={starting}
                    />
                  </div>
                </div>

                <div className="space-y-1.5">
                  <label htmlFor="idc-source-batch" className="text-sm font-medium">
                    批次
                  </label>
                  <Input
                    id="idc-source-batch"
                    value={sourceBatch}
                    onChange={(event) => setSourceBatch(event.target.value)}
                    placeholder="可选"
                    disabled={starting}
                  />
                </div>

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
              <Button asChild className="w-full">
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
                    onClick={handleCopyExternalCallbackHelper}
                  >
                    <Copy className="h-4 w-4" />
                    Windows 捕获器
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
                <Button type="button" onClick={() => void handleDialogOpenChange(false)}>
                  关闭
                </Button>
              )}
            </DialogFooter>
          </div>
        )}
      </DialogContent>
    </Dialog>
  )
}
