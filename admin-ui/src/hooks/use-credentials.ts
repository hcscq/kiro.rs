import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  getCredentials,
  setCredentialDisabled,
  setCredentialMaxConcurrency,
  setCredentialRateLimitConfig,
  setCredentialModelPolicy,
  clearCredentialRuntimeModelRestrictions,
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
  SetCredentialModelPolicyRequest,
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
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['credentials'] })
    },
  })
}

// 设置凭据级 token bucket 参数
export function useSetCredentialRateLimitConfig() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      id,
      rateLimitBucketCapacity,
      rateLimitRefillPerSecond,
    }: {
      id: number
      rateLimitBucketCapacity: number | null
      rateLimitRefillPerSecond: number | null
    }) =>
      setCredentialRateLimitConfig(id, {
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
