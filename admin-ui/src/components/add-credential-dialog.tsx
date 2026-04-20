import { useMemo, useState } from 'react'
import { toast } from 'sonner'
import {
  AccountTypeInput,
  ModelSelector,
  collectAccountTypeSuggestions,
} from '@/components/model-policy-controls'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  useAddCredential,
  useCredentials,
  useModelCapabilitiesConfig,
  useModelCatalog,
} from '@/hooks/use-credentials'
import { extractErrorMessage } from '@/lib/utils'

interface AddCredentialDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

type AuthMethod = 'social' | 'idc'

export function AddCredentialDialog({ open, onOpenChange }: AddCredentialDialogProps) {
  const [refreshToken, setRefreshToken] = useState('')
  const [authMethod, setAuthMethod] = useState<AuthMethod>('social')
  const [authRegion, setAuthRegion] = useState('')
  const [apiRegion, setApiRegion] = useState('')
  const [clientId, setClientId] = useState('')
  const [clientSecret, setClientSecret] = useState('')
  const [priority, setPriority] = useState('0')
  const [maxConcurrency, setMaxConcurrency] = useState('')
  const [rateLimitBucketCapacity, setRateLimitBucketCapacity] = useState('')
  const [rateLimitRefillPerSecond, setRateLimitRefillPerSecond] = useState('')
  const [machineId, setMachineId] = useState('')
  const [accountType, setAccountType] = useState('')
  const [allowedModels, setAllowedModels] = useState<string[]>([])
  const [blockedModels, setBlockedModels] = useState<string[]>([])
  const [proxyUrl, setProxyUrl] = useState('')
  const [proxyUsername, setProxyUsername] = useState('')
  const [proxyPassword, setProxyPassword] = useState('')

  const { mutate, isPending } = useAddCredential()
  const { data: credentialsData } = useCredentials()
  const { data: modelCapabilitiesData } = useModelCapabilitiesConfig()
  const { data: modelCatalogData } = useModelCatalog()

  const accountTypeSuggestions = useMemo(
    () =>
      collectAccountTypeSuggestions(
        credentialsData?.credentials,
        modelCapabilitiesData?.accountTypePolicies,
        modelCapabilitiesData?.accountTypeDispatchPolicies,
        modelCapabilitiesData?.standardAccountTypePresets
      ),
    [
      credentialsData?.credentials,
      modelCapabilitiesData?.accountTypePolicies,
      modelCapabilitiesData?.accountTypeDispatchPolicies,
      modelCapabilitiesData?.standardAccountTypePresets,
    ]
  )
  const modelCatalog = modelCatalogData?.models ?? []
  const standardAccountTypePresets = modelCapabilitiesData?.standardAccountTypePresets ?? []

  const resetForm = () => {
    setRefreshToken('')
    setAuthMethod('social')
    setAuthRegion('')
    setApiRegion('')
    setClientId('')
    setClientSecret('')
    setPriority('0')
    setMaxConcurrency('')
    setRateLimitBucketCapacity('')
    setRateLimitRefillPerSecond('')
    setMachineId('')
    setAccountType('')
    setAllowedModels([])
    setBlockedModels([])
    setProxyUrl('')
    setProxyUsername('')
    setProxyPassword('')
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()

    // 验证必填字段
    if (!refreshToken.trim()) {
      toast.error('请输入 Refresh Token')
      return
    }

    // IdC/Builder-ID/IAM 需要额外字段
    if (authMethod === 'idc' && (!clientId.trim() || !clientSecret.trim())) {
      toast.error('IdC/Builder-ID/IAM 认证需要填写 Client ID 和 Client Secret')
      return
    }

    const parsedMaxConcurrency = maxConcurrency.trim()
      ? parseInt(maxConcurrency, 10)
      : undefined
    if (
      parsedMaxConcurrency !== undefined &&
      (!Number.isInteger(parsedMaxConcurrency) || parsedMaxConcurrency <= 0)
    ) {
      toast.error('并发上限必须是大于 0 的整数')
      return
    }

    const parsedRateLimitBucketCapacity = rateLimitBucketCapacity.trim()
      ? Number.parseFloat(rateLimitBucketCapacity)
      : undefined
    const parsedRateLimitRefillPerSecond = rateLimitRefillPerSecond.trim()
      ? Number.parseFloat(rateLimitRefillPerSecond)
      : undefined
    if (
      parsedRateLimitBucketCapacity !== undefined &&
      (!Number.isFinite(parsedRateLimitBucketCapacity) || parsedRateLimitBucketCapacity < 0)
    ) {
      toast.error('Bucket 容量必须是大于等于 0 的数字')
      return
    }
    if (
      parsedRateLimitRefillPerSecond !== undefined &&
      (!Number.isFinite(parsedRateLimitRefillPerSecond) || parsedRateLimitRefillPerSecond < 0)
    ) {
      toast.error('回填速率必须是大于等于 0 的数字')
      return
    }

    mutate(
      {
        refreshToken: refreshToken.trim(),
        authMethod,
        authRegion: authRegion.trim() || undefined,
        apiRegion: apiRegion.trim() || undefined,
        clientId: clientId.trim() || undefined,
        clientSecret: clientSecret.trim() || undefined,
        priority: parseInt(priority) || 0,
        maxConcurrency: parsedMaxConcurrency,
        rateLimitBucketCapacity: parsedRateLimitBucketCapacity,
        rateLimitRefillPerSecond: parsedRateLimitRefillPerSecond,
        machineId: machineId.trim() || undefined,
        accountType: accountType.trim() || undefined,
        allowedModels: allowedModels.length ? allowedModels : undefined,
        blockedModels: blockedModels.length ? blockedModels : undefined,
        proxyUrl: proxyUrl.trim() || undefined,
        proxyUsername: proxyUsername.trim() || undefined,
        proxyPassword: proxyPassword.trim() || undefined,
      },
      {
        onSuccess: (data) => {
          toast.success(data.message)
          onOpenChange(false)
          resetForm()
        },
        onError: (error: unknown) => {
          toast.error(`添加失败: ${extractErrorMessage(error)}`)
        },
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-3xl max-h-[85vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>添加凭据</DialogTitle>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="flex flex-col min-h-0 flex-1">
          <div className="space-y-4 py-4 overflow-y-auto flex-1 pr-1">
            {/* Refresh Token */}
            <div className="space-y-2">
              <label htmlFor="refreshToken" className="text-sm font-medium">
                Refresh Token <span className="text-red-500">*</span>
              </label>
              <Input
                id="refreshToken"
                type="password"
                placeholder="请输入 Refresh Token"
                value={refreshToken}
                onChange={(e) => setRefreshToken(e.target.value)}
                disabled={isPending}
              />
            </div>

            {/* 认证方式 */}
            <div className="space-y-2">
              <label htmlFor="authMethod" className="text-sm font-medium">
                认证方式
              </label>
              <select
                id="authMethod"
                value={authMethod}
                onChange={(e) => setAuthMethod(e.target.value as AuthMethod)}
                disabled={isPending}
                className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <option value="social">Social</option>
                <option value="idc">IdC/Builder-ID/IAM</option>
              </select>
            </div>

            {/* Region 配置 */}
            <div className="space-y-2">
              <label className="text-sm font-medium">Region 配置</label>
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <Input
                    id="authRegion"
                    placeholder="Auth Region"
                    value={authRegion}
                    onChange={(e) => setAuthRegion(e.target.value)}
                    disabled={isPending}
                  />
                </div>
                <div>
                  <Input
                    id="apiRegion"
                    placeholder="API Region"
                    value={apiRegion}
                    onChange={(e) => setApiRegion(e.target.value)}
                    disabled={isPending}
                  />
                </div>
              </div>
              <p className="text-xs text-muted-foreground">
                均可留空使用全局配置。Auth Region 用于 Token 刷新，API Region 用于 API 请求
              </p>
            </div>

            {/* IdC/Builder-ID/IAM 额外字段 */}
            {authMethod === 'idc' && (
              <>
                <div className="space-y-2">
                  <label htmlFor="clientId" className="text-sm font-medium">
                    Client ID <span className="text-red-500">*</span>
                  </label>
                  <Input
                    id="clientId"
                    placeholder="请输入 Client ID"
                    value={clientId}
                    onChange={(e) => setClientId(e.target.value)}
                    disabled={isPending}
                  />
                </div>
                <div className="space-y-2">
                  <label htmlFor="clientSecret" className="text-sm font-medium">
                    Client Secret <span className="text-red-500">*</span>
                  </label>
                  <Input
                    id="clientSecret"
                    type="password"
                    placeholder="请输入 Client Secret"
                    value={clientSecret}
                    onChange={(e) => setClientSecret(e.target.value)}
                    disabled={isPending}
                  />
                </div>
              </>
            )}

            {/* 优先级 */}
            <div className="space-y-2">
              <label htmlFor="priority" className="text-sm font-medium">
                优先级
              </label>
              <Input
                id="priority"
                type="number"
                min="0"
                placeholder="数字越小优先级越高"
                value={priority}
                onChange={(e) => setPriority(e.target.value)}
                disabled={isPending}
              />
              <p className="text-xs text-muted-foreground">
                数字越小优先级越高，默认为 0
              </p>
            </div>

            <div className="space-y-2">
              <label htmlFor="maxConcurrency" className="text-sm font-medium">
                并发上限
              </label>
              <Input
                id="maxConcurrency"
                type="number"
                min="1"
                placeholder="留空表示不限制"
                value={maxConcurrency}
                onChange={(e) => setMaxConcurrency(e.target.value)}
                disabled={isPending}
              />
              <p className="text-xs text-muted-foreground">
                每个账号允许同时处理的请求数。达到上限后会切到其他可用账号
              </p>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">凭据级限速覆盖</label>
              <div className="grid grid-cols-2 gap-2">
                <Input
                  id="rateLimitBucketCapacity"
                  type="number"
                  min="0"
                  step="0.1"
                  placeholder="Bucket 容量"
                  value={rateLimitBucketCapacity}
                  onChange={(e) => setRateLimitBucketCapacity(e.target.value)}
                  disabled={isPending}
                />
                <Input
                  id="rateLimitRefillPerSecond"
                  type="number"
                  min="0"
                  step="0.1"
                  placeholder="回填速率 token/s"
                  value={rateLimitRefillPerSecond}
                  onChange={(e) => setRateLimitRefillPerSecond(e.target.value)}
                  disabled={isPending}
                />
              </div>
              <p className="text-xs text-muted-foreground">
                留空表示跟随全局，填 `0` 表示只对该账号禁用 token bucket
              </p>
            </div>

            {/* Machine ID */}
            <div className="space-y-2">
              <label htmlFor="machineId" className="text-sm font-medium">
                Machine ID
              </label>
              <Input
                id="machineId"
                placeholder="留空使用配置中字段, 否则由刷新Token自动派生"
                value={machineId}
                onChange={(e) => setMachineId(e.target.value)}
                disabled={isPending}
              />
              <p className="text-xs text-muted-foreground">
                可选，64 位十六进制字符串，留空使用配置中字段, 否则由刷新Token自动派生
              </p>
            </div>

            <AccountTypeInput
              id="accountType"
              label="账号类型"
              value={accountType}
              onChange={setAccountType}
              suggestions={accountTypeSuggestions}
              standardAccountTypePresets={standardAccountTypePresets}
              placeholder="优先从已有账号类型中选择，也可直接新建"
              description="可选。优先选择内置标准类型；若需特殊灰度，可使用 `power-custom`、`pro-plus-canary` 这类衍生命名。"
              disabled={isPending}
            />

            <ModelSelector
              label="账号级额外允许模型"
              selectedValues={allowedModels}
              onChange={setAllowedModels}
              options={modelCatalog}
              description="建议优先从候选列表多选；如果目标模型尚未收录，可在下方手动补充。"
              disabled={isPending}
            />

            <ModelSelector
              label="账号级额外禁用模型"
              selectedValues={blockedModels}
              onChange={setBlockedModels}
              options={modelCatalog}
              description="显式禁用优先级最高，可用于覆盖账号类型默认策略。"
              disabled={isPending}
            />

            {/* 代理配置 */}
            <div className="space-y-2">
              <label className="text-sm font-medium">代理配置</label>
              <Input
                id="proxyUrl"
                placeholder='代理 URL（留空使用全局配置，"direct" 不使用代理）'
                value={proxyUrl}
                onChange={(e) => setProxyUrl(e.target.value)}
                disabled={isPending}
              />
              <div className="grid grid-cols-2 gap-2">
                <Input
                  id="proxyUsername"
                  placeholder="代理用户名"
                  value={proxyUsername}
                  onChange={(e) => setProxyUsername(e.target.value)}
                  disabled={isPending}
                />
                <Input
                  id="proxyPassword"
                  type="password"
                  placeholder="代理密码"
                  value={proxyPassword}
                  onChange={(e) => setProxyPassword(e.target.value)}
                  disabled={isPending}
                />
              </div>
              <p className="text-xs text-muted-foreground">
                留空使用全局代理。输入 "direct" 可显式不使用代理
              </p>
            </div>
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={isPending}
            >
              取消
            </Button>
            <Button type="submit" disabled={isPending}>
              {isPending ? '添加中...' : '添加'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}
