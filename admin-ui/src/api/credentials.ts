import axios from 'axios'
import { storage } from '@/lib/storage'
import type {
  CredentialsStatusResponse,
  BalanceResponse,
  SuccessResponse,
  SetDisabledRequest,
  SetPriorityRequest,
  SetCredentialRateLimitConfigRequest,
  SetCredentialModelPolicyRequest,
  AddCredentialRequest,
  AddCredentialResponse,
  LoadBalancingConfigResponse,
  ModelCatalogResponse,
  ModelCapabilitiesConfigResponse,
  AccountTypeDispatchPolicy,
  ModelSupportPolicy,
  StandardAccountTypePreset,
  UpdateLoadBalancingConfigRequest,
  UpdateModelCapabilitiesConfigRequest,
} from '@/types/api'

// 创建 axios 实例
const api = axios.create({
  baseURL: '/api/admin',
  headers: {
    'Content-Type': 'application/json',
  },
})

// 请求拦截器添加 API Key
api.interceptors.request.use((config) => {
  const apiKey = storage.getApiKey()
  if (apiKey) {
    config.headers['x-api-key'] = apiKey
  }
  return config
})

function normalizeStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return []
  }

  return value.filter((item): item is string => typeof item === 'string')
}

function normalizeModelSupportPolicy(
  policy: ModelSupportPolicy | null | undefined
): ModelSupportPolicy {
  return {
    allowedModels: normalizeStringArray(policy?.allowedModels),
    blockedModels: normalizeStringArray(policy?.blockedModels),
  }
}

function normalizeAccountTypeDispatchPolicy(
  policy: AccountTypeDispatchPolicy | null | undefined
): AccountTypeDispatchPolicy {
  return {
    maxConcurrency:
      typeof policy?.maxConcurrency === 'number' ? policy.maxConcurrency : null,
    rateLimitBucketCapacity:
      typeof policy?.rateLimitBucketCapacity === 'number'
        ? policy.rateLimitBucketCapacity
        : null,
    rateLimitRefillPerSecond:
      typeof policy?.rateLimitRefillPerSecond === 'number'
        ? policy.rateLimitRefillPerSecond
        : null,
  }
}

function normalizeStandardAccountTypePreset(
  preset: StandardAccountTypePreset | null | undefined
): StandardAccountTypePreset {
  return {
    id: typeof preset?.id === 'string' ? preset.id : '',
    displayName: typeof preset?.displayName === 'string' ? preset.displayName : '',
    description: typeof preset?.description === 'string' ? preset.description : '',
    subscriptionTitleExamples: normalizeStringArray(preset?.subscriptionTitleExamples),
    recommendedPolicy: preset?.recommendedPolicy
      ? normalizeModelSupportPolicy(preset.recommendedPolicy)
      : null,
    recommendedDispatchPolicy: preset?.recommendedDispatchPolicy
      ? normalizeAccountTypeDispatchPolicy(preset.recommendedDispatchPolicy)
      : null,
  }
}

function normalizeModelCapabilitiesConfigResponse(
  value: ModelCapabilitiesConfigResponse
): ModelCapabilitiesConfigResponse {
  const accountTypePolicies = Object.fromEntries(
    Object.entries(value?.accountTypePolicies ?? {}).map(([accountType, policy]) => [
      accountType,
      normalizeModelSupportPolicy(policy),
    ])
  )
  const accountTypeDispatchPolicies = Object.fromEntries(
    Object.entries(value?.accountTypeDispatchPolicies ?? {}).map(([accountType, policy]) => [
      accountType,
      normalizeAccountTypeDispatchPolicy(policy),
    ])
  )

  const standardAccountTypePresets = Array.isArray(value?.standardAccountTypePresets)
    ? value.standardAccountTypePresets
        .map((preset) => normalizeStandardAccountTypePreset(preset))
        .filter((preset) => preset.id)
    : []

  return {
    accountTypePolicies,
    accountTypeDispatchPolicies,
    standardAccountTypePresets,
  }
}

// 获取所有凭据状态
export async function getCredentials(): Promise<CredentialsStatusResponse> {
  const { data } = await api.get<CredentialsStatusResponse>('/credentials')
  return data
}

// 设置凭据禁用状态
export async function setCredentialDisabled(
  id: number,
  disabled: boolean
): Promise<SuccessResponse> {
  const { data } = await api.post<SuccessResponse>(
    `/credentials/${id}/disabled`,
    { disabled } as SetDisabledRequest
  )
  return data
}

// 设置凭据优先级
export async function setCredentialPriority(
  id: number,
  priority: number
): Promise<SuccessResponse> {
  const { data } = await api.post<SuccessResponse>(
    `/credentials/${id}/priority`,
    { priority } as SetPriorityRequest
  )
  return data
}

// 设置凭据并发上限
export async function setCredentialMaxConcurrency(
  id: number,
  maxConcurrency: number | null
): Promise<SuccessResponse> {
  const { data } = await api.post<SuccessResponse>(
    `/credentials/${id}/max-concurrency`,
    { maxConcurrency }
  )
  return data
}

// 设置凭据级 token bucket 参数
export async function setCredentialRateLimitConfig(
  id: number,
  payload: SetCredentialRateLimitConfigRequest
): Promise<SuccessResponse> {
  const { data } = await api.post<SuccessResponse>(
    `/credentials/${id}/rate-limit-config`,
    payload
  )
  return data
}

// 设置凭据级模型策略
export async function setCredentialModelPolicy(
  id: number,
  payload: SetCredentialModelPolicyRequest
): Promise<SuccessResponse> {
  const { data } = await api.post<SuccessResponse>(
    `/credentials/${id}/model-policy`,
    payload
  )
  return data
}

// 清除运行时模型限制
export async function clearCredentialRuntimeModelRestrictions(
  id: number
): Promise<SuccessResponse> {
  const { data } = await api.post<SuccessResponse>(
    `/credentials/${id}/runtime-model-restrictions/clear`
  )
  return data
}

// 重置失败计数
export async function resetCredentialFailure(
  id: number
): Promise<SuccessResponse> {
  const { data } = await api.post<SuccessResponse>(`/credentials/${id}/reset`)
  return data
}

// 强制刷新 Token
export async function forceRefreshToken(
  id: number
): Promise<SuccessResponse> {
  const { data } = await api.post<SuccessResponse>(`/credentials/${id}/refresh`)
  return data
}

// 获取凭据余额
export async function getCredentialBalance(id: number): Promise<BalanceResponse> {
  const { data } = await api.get<BalanceResponse>(`/credentials/${id}/balance`)
  return data
}

// 添加新凭据
export async function addCredential(
  req: AddCredentialRequest
): Promise<AddCredentialResponse> {
  const { data } = await api.post<AddCredentialResponse>('/credentials', req)
  return data
}

// 删除凭据
export async function deleteCredential(id: number): Promise<SuccessResponse> {
  const { data } = await api.delete<SuccessResponse>(`/credentials/${id}`)
  return data
}

// 获取负载均衡模式
export async function getLoadBalancingMode(): Promise<LoadBalancingConfigResponse> {
  const { data } = await api.get<LoadBalancingConfigResponse>('/config/load-balancing')
  return data
}

// 设置负载均衡模式
export async function setLoadBalancingMode(
  payload: UpdateLoadBalancingConfigRequest
): Promise<LoadBalancingConfigResponse> {
  const { data } = await api.put<LoadBalancingConfigResponse>('/config/load-balancing', payload)
  return data
}

export async function getModelCapabilitiesConfig(): Promise<ModelCapabilitiesConfigResponse> {
  const { data } = await api.get<ModelCapabilitiesConfigResponse>('/config/model-capabilities')
  return normalizeModelCapabilitiesConfigResponse(data)
}

export async function getModelCatalog(): Promise<ModelCatalogResponse> {
  const { data } = await api.get<ModelCatalogResponse>('/config/model-catalog')
  return data
}

export async function setModelCapabilitiesConfig(
  payload: UpdateModelCapabilitiesConfigRequest
): Promise<ModelCapabilitiesConfigResponse> {
  const { data } = await api.put<ModelCapabilitiesConfigResponse>(
    '/config/model-capabilities',
    payload
  )
  return normalizeModelCapabilitiesConfigResponse(data)
}
