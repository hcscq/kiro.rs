import { Check, X } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { useCredentialGroupsConfig } from '@/hooks/use-credentials'
import { formatCredentialGroupsInput, normalizeCredentialGroups } from '@/lib/credential-groups'
import { cn } from '@/lib/utils'

interface CredentialGroupPickerProps {
  id?: string
  value: string
  onChange: (value: string) => void
  disabled?: boolean
  className?: string
  compact?: boolean
}

function groupLabel(name: string, displayName?: string | null) {
  return displayName?.trim() || name
}

export function CredentialGroupPicker({
  id,
  value,
  onChange,
  disabled = false,
  className,
  compact = false,
}: CredentialGroupPickerProps) {
  const { data } = useCredentialGroupsConfig()
  const selected = normalizeCredentialGroups(value)
  const selectedSet = new Set(selected)
  const groups = (data?.groups ?? [])
    .filter((group) => group.enabled)
    .sort((a, b) => a.name.localeCompare(b.name))
  const knownNames = new Set(groups.map((group) => group.name))
  const usageByName = new Map((data?.usage ?? []).map((item) => [item.name, item]))
  const unknownSelected = selected.filter((name) => !knownNames.has(name))
  const uncoveredSelected = selected.filter((name) => {
    const usage = usageByName.get(name)
    return knownNames.has(name) && !data?.legacyFullAccessKey && (usage?.apiKeyCount ?? 0) === 0
  })

  const setSelected = (next: string[]) => {
    onChange(formatCredentialGroupsInput(next))
  }

  const toggleGroup = (name: string) => {
    if (disabled) return
    if (selectedSet.has(name)) {
      setSelected(selected.filter((item) => item !== name))
    } else {
      setSelected([...selected, name])
    }
  }

  const removeGroup = (name: string) => {
    if (disabled) return
    setSelected(selected.filter((item) => item !== name))
  }

  return (
    <div id={id} className={cn('space-y-2', className)}>
      <div className="flex flex-wrap gap-2">
        {groups.map((group) => {
          const checked = selectedSet.has(group.name)
          return (
            <Button
              key={group.name}
              type="button"
              variant={checked ? 'secondary' : 'outline'}
              size="sm"
              className={cn('h-8 gap-1.5 px-2.5', compact && 'h-7 px-2 text-xs')}
              disabled={disabled}
              onClick={() => toggleGroup(group.name)}
            >
              {checked && <Check className="h-3.5 w-3.5" />}
              {groupLabel(group.name, group.displayName)}
            </Button>
          )
        })}
        {groups.length === 0 && (
          <Badge variant="outline" className="h-8 rounded-md px-2.5">
            default
          </Badge>
        )}
      </div>
      {unknownSelected.length > 0 && (
        <div className="flex flex-wrap gap-2">
          {unknownSelected.map((name) => (
            <button
              key={name}
              type="button"
              className="inline-flex h-7 items-center gap-1 rounded-md border border-destructive/40 bg-destructive/10 px-2 text-xs text-destructive"
              disabled={disabled}
              onClick={() => removeGroup(name)}
            >
              {name}
              <X className="h-3 w-3" />
            </button>
          ))}
        </div>
      )}
      {uncoveredSelected.length > 0 && (
        <p className="text-xs text-amber-600">
          无 scoped API Key 覆盖: {uncoveredSelected.join(', ')}
        </p>
      )}
    </div>
  )
}
