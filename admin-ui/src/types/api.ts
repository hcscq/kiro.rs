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
  accountType?: string | null
  resolvedAccountType?: string | null
  accountTypeSource?: 'credential' | 'subscription-title' | null
  standardAccountType?: string | null
  allowedModels?: string[]
  blockedModels?: string[]
  runtimeModelRestrictions?: RuntimeModelRestriction[]
  importedAt?: string | null
  refreshTokenHash?: string
  successCount: number
  lastUsedAt: string | null
  inFlight: number
  maxConcurrency?: number | null
  maxConcurrencyOverride?: number | null
  maxConcurrencySource?: 'credential' | 'account-type' | 'global-default' | null
  hasProxy: boolean
  proxyUrl?: string
  refreshFailureCount: number
  disabledReason?: string
  cooldownRemainingMs?: number | null
  rateLimitBucketTokens?: number | null
  rateLimitBucketCapacity?: number | null
  rateLimitBucketCapacityOverride?: number | null
  rateLimitBucketCapacitySource?: 'credential' | 'account-type' | 'global-default' | null
  rateLimitRefillPerSecond?: number | null
  rateLimitRefillPerSecondOverride?: number | null
  rateLimitRefillPerSecondSource?: 'credential' | 'account-type' | 'global-default' | null
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

export interface RuntimeModelRestriction {
  model: string
  expiresAt: string
}

export interface ModelSupportPolicy {
  allowedModels: string[]
  blockedModels: string[]
}

export interface AccountTypeDispatchPolicy {
  maxConcurrency?: number | null
  rateLimitBucketCapacity?: number | null
  rateLimitRefillPerSecond?: number | null
}

export interface StandardAccountTypePreset {
  id: string
  displayName: string
  description: string
  subscriptionTitleExamples: string[]
  recommendedPolicy?: ModelSupportPolicy | null
  recommendedDispatchPolicy?: AccountTypeDispatchPolicy | null
}

export interface ModelCatalogItem {
  apiId: string
  policyId: string
  displayName: string
}

export interface ModelCatalogResponse {
  models: ModelCatalogItem[]
}

export interface SetCredentialModelPolicyRequest {
  accountType?: string | null
  allowedModels?: string[] | null
  blockedModels?: string[] | null
  clearRuntimeModelRestrictions?: boolean
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
  accountType?: string
  allowedModels?: string[]
  blockedModels?: string[]
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

export interface RequestWeightingConfig {
  enabled: boolean
  baseWeight: number
  maxWeight: number
  toolsBonus: number
  largeMaxTokensThreshold: number
  largeMaxTokensBonus: number
  largeInputTokensThreshold: number
  largeInputTokensBonus: number
  veryLargeInputTokensThreshold: number
  veryLargeInputTokensBonus: number
  thinkingBonus: number
  heavyThinkingBudgetThreshold: number
  heavyThinkingBudgetBonus: number
}

export interface LoadBalancingConfigResponse {
  mode: 'priority' | 'balanced'
  queueMaxSize: number
  queueMaxWaitMs: number
  rateLimitCooldownMs: number
  modelCooldownEnabled: boolean
  defaultMaxConcurrency: number | null
  rateLimitBucketCapacity: number
  rateLimitRefillPerSecond: number
  rateLimitRefillMinPerSecond: number
  rateLimitRefillRecoveryStepPerSuccess: number
  rateLimitRefillBackoffFactor: number
  requestWeighting: RequestWeightingConfig
  waitingRequests: number
}

export interface UpdateLoadBalancingConfigRequest {
  mode?: 'priority' | 'balanced'
  queueMaxSize?: number
  queueMaxWaitMs?: number
  rateLimitCooldownMs?: number
  modelCooldownEnabled?: boolean
  defaultMaxConcurrency?: number
  rateLimitBucketCapacity?: number
  rateLimitRefillPerSecond?: number
  rateLimitRefillMinPerSecond?: number
  rateLimitRefillRecoveryStepPerSuccess?: number
  rateLimitRefillBackoffFactor?: number
  requestWeighting?: RequestWeightingConfig
}

export interface ModelCapabilitiesConfigResponse {
  accountTypePolicies: Record<string, ModelSupportPolicy>
  accountTypeDispatchPolicies: Record<string, AccountTypeDispatchPolicy>
  standardAccountTypePresets: StandardAccountTypePreset[]
}

export interface UpdateModelCapabilitiesConfigRequest {
  accountTypePolicies?: Record<string, ModelSupportPolicy>
  accountTypeDispatchPolicies?: Record<string, AccountTypeDispatchPolicy>
}
