const API_KEY_STORAGE_KEY = 'adminApiKey'
const RECENT_IDC_AUTH_REGIONS_STORAGE_KEY = 'recentIdcAuthRegions'
const RECENT_IDC_START_URLS_STORAGE_KEY = 'recentIdcStartUrls'
const RECENT_IDC_LOGIN_PAIRS_STORAGE_KEY = 'recentIdcLoginPairs'
const MAX_RECENT_IDC_ITEMS = 8

export interface RecentIdcLoginPair {
  startUrl: string
  authRegion: string
}

function readJsonArray<T>(key: string): T[] {
  try {
    const raw = localStorage.getItem(key)
    if (!raw) return []
    const parsed = JSON.parse(raw)
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

function writeJsonArray<T>(key: string, value: T[]): void {
  localStorage.setItem(key, JSON.stringify(value))
}

function normalizeRegion(region: string): string {
  return region.trim().toLowerCase()
}

function normalizeStartUrl(startUrl: string): string {
  const trimmed = startUrl.trim()
  if (!trimmed) return ''
  return trimmed.replace(/\/+$/, '')
}

function pushRecentString(key: string, value: string): void {
  const normalized = key === RECENT_IDC_AUTH_REGIONS_STORAGE_KEY
    ? normalizeRegion(value)
    : normalizeStartUrl(value)
  if (!normalized) return

  const existing = readJsonArray<string>(key)
  const next = [
    normalized,
    ...existing.filter((item) => item.trim().toLowerCase() !== normalized.toLowerCase()),
  ].slice(0, MAX_RECENT_IDC_ITEMS)

  writeJsonArray(key, next)
}

export const storage = {
  getApiKey: () => localStorage.getItem(API_KEY_STORAGE_KEY),
  setApiKey: (key: string) => localStorage.setItem(API_KEY_STORAGE_KEY, key),
  removeApiKey: () => localStorage.removeItem(API_KEY_STORAGE_KEY),
  getRecentIdcAuthRegions: () =>
    readJsonArray<string>(RECENT_IDC_AUTH_REGIONS_STORAGE_KEY)
      .map(normalizeRegion)
      .filter(Boolean),
  addRecentIdcAuthRegion: (region: string) => {
    pushRecentString(RECENT_IDC_AUTH_REGIONS_STORAGE_KEY, region)
  },
  getRecentIdcStartUrls: () =>
    readJsonArray<string>(RECENT_IDC_START_URLS_STORAGE_KEY)
      .map(normalizeStartUrl)
      .filter(Boolean),
  addRecentIdcStartUrl: (startUrl: string) => {
    pushRecentString(RECENT_IDC_START_URLS_STORAGE_KEY, startUrl)
  },
  getRecentIdcLoginPairs: () =>
    readJsonArray<RecentIdcLoginPair>(RECENT_IDC_LOGIN_PAIRS_STORAGE_KEY)
      .map((pair) => ({
        startUrl: normalizeStartUrl(pair.startUrl || ''),
        authRegion: normalizeRegion(pair.authRegion || ''),
      }))
      .filter((pair) => pair.startUrl && pair.authRegion),
  addRecentIdcLoginPair: (pair: RecentIdcLoginPair) => {
    const startUrl = normalizeStartUrl(pair.startUrl)
    const authRegion = normalizeRegion(pair.authRegion)
    if (!startUrl || !authRegion) return

    const existing = storage.getRecentIdcLoginPairs()
    const next = [
      { startUrl, authRegion },
      ...existing.filter((item) => item.startUrl.toLowerCase() !== startUrl.toLowerCase()),
    ].slice(0, MAX_RECENT_IDC_ITEMS)

    writeJsonArray(RECENT_IDC_LOGIN_PAIRS_STORAGE_KEY, next)
    storage.addRecentIdcStartUrl(startUrl)
    storage.addRecentIdcAuthRegion(authRegion)
  },
  getRecentIdcAuthRegionForStartUrl: (startUrl: string) => {
    const normalized = normalizeStartUrl(startUrl)
    if (!normalized) return undefined

    return storage
      .getRecentIdcLoginPairs()
      .find((pair) => pair.startUrl.toLowerCase() === normalized.toLowerCase())
      ?.authRegion
  },
}
