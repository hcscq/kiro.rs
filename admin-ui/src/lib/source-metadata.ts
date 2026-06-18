import type { CredentialStatusItem } from '@/types/api'

function pad2(value: number): string {
  return String(value).padStart(2, '0')
}

export function formatDefaultSourceBatch(date = new Date()): string {
  const year = date.getFullYear()
  const month = pad2(date.getMonth() + 1)
  const day = pad2(date.getDate())
  const hour = pad2(date.getHours())
  return `${year}${month}${day}${hour}1`
}

export function collectSourceSupplierSuggestions(
  credentials: CredentialStatusItem[] | undefined
): string[] {
  const values = new Set<string>()

  for (const credential of credentials ?? []) {
    const name = credential.sourceSupplierName?.trim()
    if (name) {
      values.add(name)
      continue
    }

    const id = credential.sourceSupplierId?.trim()
    if (id) {
      values.add(id)
    }
  }

  return Array.from(values).sort((left, right) => left.localeCompare(right, 'zh-CN'))
}

export function normalizedSourceString(value: string | undefined): string | undefined {
  const trimmed = value?.trim()
  return trimmed || undefined
}
