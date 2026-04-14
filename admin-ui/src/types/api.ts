// 凭据状态响应
export interface CredentialsStatusResponse {
  total: number
  available: number
  dispatchable: number
  currentId: number
  credentials: CredentialStatusItem[]
}

// 单个凭据状态
export interface CredentialStatusItem {
  id: number
  priority: number
  disabled: boolean
  failureCount: number
  isCurrent: boolean
  expiresAt: string | null
  authMethod: string | null
  hasProfileArn: boolean
  email?: string
  subscriptionTitle?: string | null
  importedAt?: string | null
  refreshTokenHash?: string
  successCount: number
  lastUsedAt: string | null
  inFlight: number
  maxConcurrency?: number | null
  hasProxy: boolean
  proxyUrl?: string
  refreshFailureCount: number
  disabledReason?: string
  cooldownRemainingMs?: number | null
  rateLimitBucketTokens?: number | null
  rateLimitBucketCapacity?: number | null
  rateLimitBucketCapacityOverride?: number | null
  rateLimitRefillPerSecond?: number | null
  rateLimitRefillPerSecondOverride?: number | null
  rateLimitRefillBasePerSecond?: number | null
  rateLimitHitStreak: number
  nextReadyInMs?: number | null
}

// 余额响应
export interface BalanceResponse {
  id: number
  subscriptionTitle: string | null
  currentUsage: number
  usageLimit: number
  remaining: number
  usagePercentage: number
  nextResetAt: number | null
}

// 成功响应
export interface SuccessResponse {
  success: boolean
  message: string
}

// 错误响应
export interface AdminErrorResponse {
  error: {
    type: string
    message: string
  }
}

// 请求类型
export interface SetDisabledRequest {
  disabled: boolean
}

export interface SetPriorityRequest {
  priority: number
}

export interface SetCredentialRateLimitConfigRequest {
  rateLimitBucketCapacity?: number | null
  rateLimitRefillPerSecond?: number | null
}

// 添加凭据请求
export interface AddCredentialRequest {
  refreshToken: string
  authMethod?: 'social' | 'idc'
  clientId?: string
  clientSecret?: string
  priority?: number
  maxConcurrency?: number
  rateLimitBucketCapacity?: number
  rateLimitRefillPerSecond?: number
  region?: string
  authRegion?: string
  apiRegion?: string
  machineId?: string
  email?: string
  proxyUrl?: string
  proxyUsername?: string
  proxyPassword?: string
}

// 添加凭据响应
export interface AddCredentialResponse {
  success: boolean
  message: string
  credentialId: number
  email?: string
}

export interface LoadBalancingConfigResponse {
  mode: 'priority' | 'balanced'
  queueMaxSize: number
  queueMaxWaitMs: number
  rateLimitCooldownMs: number
  defaultMaxConcurrency: number | null
  rateLimitBucketCapacity: number
  rateLimitRefillPerSecond: number
  rateLimitRefillMinPerSecond: number
  rateLimitRefillRecoveryStepPerSuccess: number
  rateLimitRefillBackoffFactor: number
  waitingRequests: number
}

export interface UpdateLoadBalancingConfigRequest {
  mode?: 'priority' | 'balanced'
  queueMaxSize?: number
  queueMaxWaitMs?: number
  rateLimitCooldownMs?: number
  defaultMaxConcurrency?: number
  rateLimitBucketCapacity?: number
  rateLimitRefillPerSecond?: number
  rateLimitRefillMinPerSecond?: number
  rateLimitRefillRecoveryStepPerSuccess?: number
  rateLimitRefillBackoffFactor?: number
}
