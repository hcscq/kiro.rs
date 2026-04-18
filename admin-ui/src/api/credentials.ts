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
  ModelCapabilitiesConfigResponse,
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
  return data
}

export async function setModelCapabilitiesConfig(
  payload: UpdateModelCapabilitiesConfigRequest
): Promise<ModelCapabilitiesConfigResponse> {
  const { data } = await api.put<ModelCapabilitiesConfigResponse>(
    '/config/model-capabilities',
    payload
  )
  return data
}
