export function normalizeCredentialGroups(values: unknown): string[] {
  const rawValues = Array.isArray(values)
    ? values
    : typeof values === 'string'
      ? values.split(/[\s,，;；]+/)
      : []

  const seen = new Set<string>()
  const groups: string[] = []

  rawValues.forEach((value) => {
    if (typeof value !== 'string') return
    const normalized = value.trim().toLowerCase()
    if (!normalized || seen.has(normalized)) return
    seen.add(normalized)
    groups.push(normalized)
  })

  return groups
}

export function formatCredentialGroupsInput(values: unknown): string {
  return normalizeCredentialGroups(values).join(', ')
}
