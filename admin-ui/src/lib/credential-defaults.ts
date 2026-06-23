import type { CredentialProxyMode } from '@/types/api'
import { formatDefaultSourceBatch } from '@/lib/source-metadata'

const CREDENTIAL_DEFAULTS_STORAGE_KEY = 'credentialDefaultsDraft:v1'

export type RateLimitCooldownMode = 'global' | 'enabled' | 'disabled'

export interface CredentialDefaultsDraft {
  priority: string
  maxConcurrency: string
  rateLimitCooldownMode: RateLimitCooldownMode
  sourceSupplierName: string
  sourceSupplierId: string
  sourceBatch: string
  accountType: string
  authRegion: string
  apiRegion: string
  profileArn: string
  machineId: string
  proxyMode: CredentialProxyMode
  proxyId: string
  proxyUrl: string
  proxyUsername: string
  autoEnableOverage: boolean
}

function stringValue(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

function proxyModeValue(value: unknown): CredentialProxyMode {
  return value === 'pool' || value === 'custom' || value === 'direct' ? value : 'auto'
}

function rateLimitCooldownModeValue(value: unknown): RateLimitCooldownMode {
  return value === 'enabled' || value === 'disabled' ? value : 'global'
}

export function createCredentialDefaultsDraft(): CredentialDefaultsDraft {
  return {
    priority: '0',
    maxConcurrency: '',
    rateLimitCooldownMode: 'global',
    sourceSupplierName: '',
    sourceSupplierId: '',
    sourceBatch: formatDefaultSourceBatch(),
    accountType: '',
    authRegion: '',
    apiRegion: '',
    profileArn: '',
    machineId: '',
    proxyMode: 'auto',
    proxyId: '',
    proxyUrl: '',
    proxyUsername: '',
    autoEnableOverage: false,
  }
}

export function readCredentialDefaultsDraft(): CredentialDefaultsDraft {
  const defaults = createCredentialDefaultsDraft()

  try {
    const raw = window.localStorage.getItem(CREDENTIAL_DEFAULTS_STORAGE_KEY)
    if (!raw) return defaults

    const parsed = JSON.parse(raw) as Partial<CredentialDefaultsDraft>
    return {
      ...defaults,
      priority: stringValue(parsed.priority) || defaults.priority,
      maxConcurrency: stringValue(parsed.maxConcurrency),
      rateLimitCooldownMode: rateLimitCooldownModeValue(parsed.rateLimitCooldownMode),
      sourceSupplierName: stringValue(parsed.sourceSupplierName),
      sourceSupplierId: stringValue(parsed.sourceSupplierId),
      sourceBatch: stringValue(parsed.sourceBatch) || defaults.sourceBatch,
      accountType: stringValue(parsed.accountType),
      authRegion: stringValue(parsed.authRegion),
      apiRegion: stringValue(parsed.apiRegion),
      profileArn: stringValue(parsed.profileArn),
      machineId: stringValue(parsed.machineId),
      proxyMode: proxyModeValue(parsed.proxyMode),
      proxyId: stringValue(parsed.proxyId),
      proxyUrl: stringValue(parsed.proxyUrl),
      proxyUsername: stringValue(parsed.proxyUsername),
      autoEnableOverage: parsed.autoEnableOverage === true,
    }
  } catch {
    return defaults
  }
}

export function persistCredentialDefaultsDraft(
  draft: Partial<CredentialDefaultsDraft>
): CredentialDefaultsDraft {
  const current = readCredentialDefaultsDraft()
  const next = {
    ...current,
    ...draft,
    sourceBatch: draft.sourceBatch === undefined
      ? current.sourceBatch
      : draft.sourceBatch || formatDefaultSourceBatch(),
  }

  try {
    window.localStorage.setItem(CREDENTIAL_DEFAULTS_STORAGE_KEY, JSON.stringify(next))
  } catch {
    // 本地存储不可用时不影响导入/登录主流程。
  }

  return next
}

export function resetCredentialDefaultsDraft(): CredentialDefaultsDraft {
  const defaults = createCredentialDefaultsDraft()
  try {
    window.localStorage.setItem(CREDENTIAL_DEFAULTS_STORAGE_KEY, JSON.stringify(defaults))
  } catch {
    // 本地存储不可用时只返回内存默认值。
  }
  return defaults
}
