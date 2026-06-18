import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  getCredentials,
  setCredentialDisabled,
  setCredentialMaxConcurrency,
  setCredentialRateLimitConfig,
  setCredentialModelPolicy,
  setCredentialSource,
  setCredentialProxy,
  getCredentialProfiles,
  setCredentialProfile,
  clearCredentialRuntimeModelRestrictions,
  clearCredentialSuspiciousActivity,
  setCredentialPriority,
  resetCredentialFailure,
  forceRefreshToken,
  getCredentialBalance,
  addCredential,
  deleteCredential,
  getLoadBalancingMode,
  getModelCatalog,
  setLoadBalancingMode,
  getModelCapabilitiesConfig,
  setModelCapabilitiesConfig,
} from '@/api/credentials'
import type {
  AddCredentialRequest,
  CredentialsStatusResponse,
  SetCredentialModelPolicyRequest,
  SetCredentialSourceRequest,
  SetCredentialProxyRequest,
  SetCredentialProfileRequest,
  UpdateLoadBalancingConfigRequest,
  UpdateModelCapabilitiesConfigRequest,
} from '@/types/api'

// 查询凭据列表
export function useCredentials() {
  return useQuery({
    queryKey: ['credentials'],
    queryFn: getCredentials,
    refetchInterval: 30000, // 每 30 秒刷新一次
  })
}

// 查询凭据余额
export function useCredentialBalance(id: number | null) {
  return useQuery({
    queryKey: ['credential-balance', id],
    queryFn: () => getCredentialBalance(id!),
    enabled: id !== null,
    retry: false, // 余额查询失败时不重试（避免重复请求被封禁的账号）
  })
}

// 查询凭据可用 Profile
export function useCredentialProfiles(id: number | null, enabled = true) {
  return useQuery({
    queryKey: ['credential-profiles', id],
    queryFn: () => getCredentialProfiles(id!),
    enabled: enabled && id !== null,
    retry: false,
  })
}

// 设置禁用状态
export function useSetDisabled() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, disabled }: { id: number; disabled: boolean }) =>
      setCredentialDisabled(id, disabled),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
    },
  })
}

// 设置优先级
export function useSetPriority() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, priority }: { id: number; priority: number }) =>
      setCredentialPriority(id, priority),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
    },
  })
}

// 设置并发上限
export function useSetMaxConcurrency() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, maxConcurrency }: { id: number; maxConcurrency: number | null }) =>
      setCredentialMaxConcurrency(id, maxConcurrency),
    onSuccess: async (_response, { id, maxConcurrency }) => {
      queryClient.setQueryData<CredentialsStatusResponse>(['credentials'], (current) => {
        if (!current) return current

        return {
          ...current,
          credentials: current.credentials.map((credential) => {
            if (credential.id !== id || maxConcurrency === null) {
              return credential
            }

            return {
              ...credential,
              maxConcurrency,
              maxConcurrencyOverride: maxConcurrency,
              maxConcurrencySource: 'credential',
            }
          }),
        }
      })
      await queryClient.invalidateQueries({ queryKey: ['credentials'] })
    },
  })
}

// 设置凭据级 token bucket 参数
export function useSetCredentialRateLimitConfig() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      id,
      rateLimitCooldownEnabled,
      rateLimitBucketCapacity,
      rateLimitRefillPerSecond,
    }: {
      id: number
      rateLimitCooldownEnabled: boolean | null
      rateLimitBucketCapacity: number | null
      rateLimitRefillPerSecond: number | null
    }) =>
      setCredentialRateLimitConfig(id, {
        rateLimitCooldownEnabled,
        rateLimitBucketCapacity,
        rateLimitRefillPerSecond,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
    },
  })
}

// 设置凭据级模型策略
export function useSetCredentialModelPolicy() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, payload }: { id: number; payload: SetCredentialModelPolicyRequest }) =>
      setCredentialModelPolicy(id, payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
      queryClient.invalidateQueries({ queryKey: ['modelCapabilities'] })
    },
  })
}

// 设置凭据来源标记
export function useSetCredentialSource() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, payload }: { id: number; payload: SetCredentialSourceRequest }) =>
      setCredentialSource(id, payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
    },
  })
}

// 设置凭据代理绑定
export function useSetCredentialProxy() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, payload }: { id: number; payload: SetCredentialProxyRequest }) =>
      setCredentialProxy(id, payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
      queryClient.invalidateQueries({ queryKey: ['loadBalancingMode'] })
    },
  })
}

// 设置凭据当前 Profile
export function useSetCredentialProfile() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      id,
      payload,
    }: {
      id: number
      payload: SetCredentialProfileRequest
    }) => setCredentialProfile(id, payload),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
      queryClient.invalidateQueries({ queryKey: ['credential-profiles', variables.id] })
      queryClient.invalidateQueries({ queryKey: ['credential-balance', variables.id] })
    },
  })
}

// 清除运行时模型限制
export function useClearCredentialRuntimeModelRestrictions() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => clearCredentialRuntimeModelRestrictions(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
    },
  })
}

// 清除 suspicious activity 标记与隔离
export function useClearCredentialSuspiciousActivity() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => clearCredentialSuspiciousActivity(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
    },
  })
}

// 重置失败计数
export function useResetFailure() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => resetCredentialFailure(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
    },
  })
}

// 强制刷新 Token
export function useForceRefreshToken() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => forceRefreshToken(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
    },
  })
}

// 添加新凭据
export function useAddCredential() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (req: AddCredentialRequest) => addCredential(req),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
      queryClient.invalidateQueries({ queryKey: ['loadBalancingMode'] })
    },
  })
}

// 删除凭据
export function useDeleteCredential() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => deleteCredential(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
      queryClient.invalidateQueries({ queryKey: ['loadBalancingMode'] })
    },
  })
}

// 获取负载均衡模式
export function useLoadBalancingMode() {
  return useQuery({
    queryKey: ['loadBalancingMode'],
    queryFn: getLoadBalancingMode,
  })
}

// 设置负载均衡模式
export function useSetLoadBalancingMode() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (payload: UpdateLoadBalancingConfigRequest) => setLoadBalancingMode(payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['loadBalancingMode'] })
    },
  })
}

export function useModelCapabilitiesConfig() {
  return useQuery({
    queryKey: ['modelCapabilities'],
    queryFn: getModelCapabilitiesConfig,
  })
}

export function useModelCatalog() {
  return useQuery({
    queryKey: ['modelCatalog'],
    queryFn: getModelCatalog,
    staleTime: Infinity,
  })
}

export function useSetModelCapabilitiesConfig() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (payload: UpdateModelCapabilitiesConfigRequest) =>
      setModelCapabilitiesConfig(payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['modelCapabilities'] })
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
    },
  })
}
