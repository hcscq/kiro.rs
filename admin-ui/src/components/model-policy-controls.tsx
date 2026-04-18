import { useEffect, useId, useMemo, useState } from 'react'
import { Plus, Search, X } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { Input } from '@/components/ui/input'
import { cn } from '@/lib/utils'
import type {
  CredentialStatusItem,
  ModelCatalogItem,
  ModelSupportPolicy,
} from '@/types/api'

function splitCustomEntries(value: string): string[] {
  return value
    .split(/[,\n]/)
    .map((item) => item.trim())
    .filter(Boolean)
}

export function collectAccountTypeSuggestions(
  credentials: CredentialStatusItem[] | undefined,
  accountTypePolicies: Record<string, ModelSupportPolicy> | undefined
): string[] {
  const values = new Set<string>()

  for (const credential of credentials ?? []) {
    const accountType = credential.accountType?.trim()
    if (accountType) {
      values.add(accountType)
    }
  }

  for (const accountType of Object.keys(accountTypePolicies ?? {})) {
    const normalized = accountType.trim()
    if (normalized) {
      values.add(normalized)
    }
  }

  return Array.from(values).sort((left, right) => left.localeCompare(right, 'zh-CN'))
}

interface AccountTypeInputProps {
  label: string
  value: string
  onChange: (value: string) => void
  suggestions: string[]
  placeholder?: string
  description?: string
  disabled?: boolean
  id?: string
}

const nativeSelectClassName =
  'flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50'

export function AccountTypeInput({
  label,
  value,
  onChange,
  suggestions,
  placeholder,
  description,
  disabled = false,
  id,
}: AccountTypeInputProps) {
  const fallbackId = useId()
  const inputId = id ?? fallbackId
  const [useCustomValue, setUseCustomValue] = useState(false)

  const normalizedSuggestions = useMemo(() => {
    return Array.from(
      new Set(
        suggestions
          .map((suggestion) => suggestion.trim())
          .filter(Boolean)
      )
    ).sort((left, right) => left.localeCompare(right, 'zh-CN'))
  }, [suggestions])

  const trimmedValue = value.trim()
  const matchesSuggestion = trimmedValue !== '' && normalizedSuggestions.includes(trimmedValue)
  const canSelectSuggestion = normalizedSuggestions.length > 0

  useEffect(() => {
    if (!canSelectSuggestion) {
      setUseCustomValue(true)
      return
    }

    if (!trimmedValue) {
      setUseCustomValue(false)
      return
    }

    setUseCustomValue(!matchesSuggestion)
  }, [canSelectSuggestion, matchesSuggestion, trimmedValue])

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-2">
        <label htmlFor={inputId} className="text-sm font-medium">
          {label}
        </label>
        {canSelectSuggestion && <Badge variant="outline">{normalizedSuggestions.length} 个候选</Badge>}
      </div>

      <div className="space-y-3 rounded-lg border border-input bg-background p-3">
        {canSelectSuggestion && (
          <div className="flex flex-wrap gap-2">
            <Button
              type="button"
              size="sm"
              variant={useCustomValue ? 'outline' : 'secondary'}
              onClick={() => setUseCustomValue(false)}
              disabled={disabled}
            >
              选择已有
            </Button>
            <Button
              type="button"
              size="sm"
              variant={useCustomValue ? 'secondary' : 'outline'}
              onClick={() => setUseCustomValue(true)}
              disabled={disabled}
            >
              自定义类型
            </Button>
          </div>
        )}

        {!useCustomValue && canSelectSuggestion ? (
          <select
            id={inputId}
            value={matchesSuggestion ? trimmedValue : ''}
            onChange={(event) => onChange(event.target.value)}
            disabled={disabled}
            className={nativeSelectClassName}
          >
            <option value="">未设置</option>
            {normalizedSuggestions.map((suggestion) => (
              <option key={suggestion} value={suggestion}>
                {suggestion}
              </option>
            ))}
          </select>
        ) : (
          <div className="space-y-2">
            <Input
              id={inputId}
              value={value}
              onChange={(event) => onChange(event.target.value)}
              placeholder={placeholder ?? '输入新的账号类型'}
              disabled={disabled}
            />
            {matchesSuggestion && (
              <p className="text-xs text-muted-foreground">
                当前输入已命中已有账号类型，切回“选择已有”即可直接复用。
              </p>
            )}
          </div>
        )}

        {!useCustomValue && !matchesSuggestion && trimmedValue && (
          <p className="text-xs text-muted-foreground">
            当前值是未收录类型，切换到“自定义类型”后可继续编辑。
          </p>
        )}
      </div>

      {description && <p className="text-xs text-muted-foreground">{description}</p>}
    </div>
  )
}

interface ModelSelectorProps {
  label: string
  selectedValues: string[]
  onChange: (values: string[]) => void
  options: ModelCatalogItem[]
  description?: string
  placeholder?: string
  disabled?: boolean
  hideHeader?: boolean
}

export function ModelSelector({
  label,
  selectedValues,
  onChange,
  options,
  description,
  placeholder = '筛选模型名称或 ID',
  disabled = false,
  hideHeader = false,
}: ModelSelectorProps) {
  const [keyword, setKeyword] = useState('')
  const [customEntry, setCustomEntry] = useState('')

  const optionMap = useMemo(() => {
    return new Map(options.map((option) => [option.policyId, option]))
  }, [options])

  const filteredOptions = useMemo(() => {
    const normalizedKeyword = keyword.trim().toLowerCase()
    if (!normalizedKeyword) {
      return options
    }

    return options.filter((option) => {
      const haystack = [
        option.displayName,
        option.policyId,
        option.apiId,
      ]
        .join(' ')
        .toLowerCase()
      return haystack.includes(normalizedKeyword)
    })
  }, [keyword, options])

  const toggleValue = (value: string, checked: boolean) => {
    if (checked) {
      if (selectedValues.includes(value)) {
        return
      }
      onChange([...selectedValues, value])
      return
    }

    onChange(selectedValues.filter((item) => item !== value))
  }

  const removeValue = (value: string) => {
    onChange(selectedValues.filter((item) => item !== value))
  }

  const addCustomEntry = () => {
    const nextValues = new Set(selectedValues)
    for (const entry of splitCustomEntries(customEntry)) {
      nextValues.add(entry)
    }
    onChange(Array.from(nextValues))
    setCustomEntry('')
  }

  return (
    <div className="space-y-2">
      {!hideHeader && (
        <div className="flex items-center justify-between gap-2">
          <label className="text-sm font-medium">{label}</label>
          <Badge variant="outline">{selectedValues.length} 已选</Badge>
        </div>
      )}
      <div className="space-y-3 rounded-lg border border-input bg-background p-3">
        {selectedValues.length > 0 ? (
          <div className="flex flex-wrap gap-2">
            {selectedValues.map((value) => {
              const option = optionMap.get(value)
              return (
                <Badge
                  key={value}
                  variant="secondary"
                  className="flex items-center gap-1 pr-1"
                >
                  <span>{option?.displayName ?? value}</span>
                  <button
                    type="button"
                    className="rounded p-0.5 hover:bg-black/10 disabled:opacity-50"
                    onClick={() => removeValue(value)}
                    disabled={disabled}
                    aria-label={`移除 ${value}`}
                  >
                    <X className="h-3 w-3" />
                  </button>
                </Badge>
              )
            })}
          </div>
        ) : (
          <div className="text-sm text-muted-foreground">尚未选择模型</div>
        )}

        <div className="relative">
          <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={keyword}
            onChange={(event) => setKeyword(event.target.value)}
            placeholder={placeholder}
            className="pl-9"
            disabled={disabled}
          />
        </div>

        <div
          className={cn(
            'max-h-48 space-y-2 overflow-y-auto rounded-md border border-dashed p-2',
            filteredOptions.length === 0 && 'flex items-center justify-center'
          )}
        >
          {filteredOptions.length === 0 ? (
            <div className="text-sm text-muted-foreground">没有匹配的内置模型</div>
          ) : (
            filteredOptions.map((option) => {
              const checked = selectedValues.includes(option.policyId)
              return (
                <label
                  key={option.policyId}
                  className={cn(
                    'flex cursor-pointer items-start gap-3 rounded-md border px-3 py-2',
                    checked ? 'border-primary bg-primary/5' : 'border-border'
                  )}
                >
                  <Checkbox
                    checked={checked}
                    onCheckedChange={(value) => toggleValue(option.policyId, Boolean(value))}
                    disabled={disabled}
                  />
                  <div className="space-y-1">
                    <div className="text-sm font-medium">{option.displayName}</div>
                    <div className="text-xs text-muted-foreground">{option.policyId}</div>
                  </div>
                </label>
              )
            })
          )}
        </div>

        <div className="space-y-2 rounded-md bg-muted/30 p-3">
          <div className="text-xs font-medium text-muted-foreground">
            兜底输入：手动补充未收录的模型 ID
          </div>
          <div className="flex gap-2">
            <Input
              value={customEntry}
              onChange={(event) => setCustomEntry(event.target.value)}
              placeholder="可粘贴一个或多个模型 ID，逗号/换行分隔"
              disabled={disabled}
              onKeyDown={(event) => {
                if (event.key === 'Enter') {
                  event.preventDefault()
                  addCustomEntry()
                }
              }}
            />
            <Button
              type="button"
              variant="outline"
              onClick={addCustomEntry}
              disabled={disabled || splitCustomEntries(customEntry).length === 0}
            >
              <Plus className="mr-2 h-4 w-4" />
              添加
            </Button>
          </div>
        </div>
      </div>
      {description && <p className="text-xs text-muted-foreground">{description}</p>}
    </div>
  )
}
