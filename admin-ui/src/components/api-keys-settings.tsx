import { useEffect, useMemo, useState } from 'react'
import { KeyRound, Plus, Save, Trash2 } from 'lucide-react'
import { toast } from 'sonner'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { CredentialGroupPicker } from '@/components/credential-group-picker'
import { useApiKeysConfig, useSetApiKeysConfig } from '@/hooks/use-credentials'
import { formatCredentialGroupsInput, normalizeCredentialGroups } from '@/lib/credential-groups'
import { extractErrorMessage } from '@/lib/utils'
import type { ApiKeyConfigItem, ApiKeyConfigUpdateItem } from '@/types/api'

interface ApiKeyDraft {
  id: string
  keyMask: string
  key: string
  allowedCredentialGroups: string
  isNew: boolean
}

function toDraft(item: ApiKeyConfigItem): ApiKeyDraft {
  return {
    id: item.id,
    keyMask: item.keyMask,
    key: '',
    allowedCredentialGroups: formatCredentialGroupsInput(item.allowedCredentialGroups),
    isNew: false,
  }
}

function newDraft(index: number): ApiKeyDraft {
  return {
    id: `api-key-${index}`,
    keyMask: '',
    key: '',
    allowedCredentialGroups: 'default',
    isNew: true,
  }
}

export function ApiKeysSettings() {
  const { data, isLoading } = useApiKeysConfig()
  const { mutate: saveApiKeys, isPending } = useSetApiKeysConfig()
  const [legacyApiKey, setLegacyApiKey] = useState('')
  const [keepLegacy, setKeepLegacy] = useState(false)
  const [drafts, setDrafts] = useState<ApiKeyDraft[]>([])

  useEffect(() => {
    setKeepLegacy(Boolean(data?.legacyFullAccessKey))
    setLegacyApiKey('')
    setDrafts((data?.keys ?? []).map(toDraft))
  }, [data])

  const existingIds = useMemo(() => new Set(drafts.map((draft) => draft.id)), [drafts])

  const updateDraft = (index: number, patch: Partial<ApiKeyDraft>) => {
    setDrafts((current) =>
      current.map((draft, draftIndex) =>
        draftIndex === index ? { ...draft, ...patch } : draft
      )
    )
  }

  const addDraft = () => {
    let index = drafts.length + 1
    while (existingIds.has(`api-key-${index}`)) {
      index += 1
    }
    setDrafts((current) => [...current, newDraft(index)])
  }

  const removeDraft = (index: number) => {
    setDrafts((current) => current.filter((_, draftIndex) => draftIndex !== index))
  }

  const handleSave = () => {
    const keys: ApiKeyConfigUpdateItem[] = []
    const ids = new Set<string>()

    for (const [index, draft] of drafts.entries()) {
      const id = draft.id.trim()
      const key = draft.key.trim()
      const allowedCredentialGroups = normalizeCredentialGroups(draft.allowedCredentialGroups)

      if (!id) {
        toast.error(`第 ${index + 1} 个 Key 缺少 ID`)
        return
      }
      if (ids.has(id)) {
        toast.error(`Key ID 重复: ${id}`)
        return
      }
      ids.add(id)
      if (draft.isNew && !key) {
        toast.error(`新 Key ${id} 需要填写密钥`)
        return
      }
      if (allowedCredentialGroups.length === 0) {
        toast.error(`Key ${id} 至少需要一个凭据分组`)
        return
      }

      keys.push({
        id,
        key: key || undefined,
        allowedCredentialGroups,
      })
    }

    const legacyKeyEnabled =
      keepLegacy && (Boolean(data?.legacyFullAccessKey) || legacyApiKey.trim().length > 0)
    if (!legacyKeyEnabled && keys.length === 0) {
      toast.error('至少需要保留一个客户端 API Key')
      return
    }

    saveApiKeys(
      {
        ...(keepLegacy
          ? legacyApiKey.trim()
            ? { legacyApiKey: legacyApiKey.trim() }
            : {}
          : { legacyApiKey: null }),
        keys,
      },
      {
        onSuccess: () => {
          toast.success('API Key 配置已保存')
          setLegacyApiKey('')
        },
        onError: (error) => toast.error(extractErrorMessage(error)),
      }
    )
  }

  if (isLoading) {
    return (
      <Card className="border-muted shadow-sm">
        <CardContent className="flex h-24 items-center justify-center">
          <div className="h-6 w-6 animate-spin rounded-full border-b-2 border-primary" />
        </CardContent>
      </Card>
    )
  }

  return (
    <Card className="border-muted shadow-sm">
      <CardHeader>
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <CardTitle>API Key 管理</CardTitle>
            <CardDescription>维护客户端 Key 与可调度凭据分组</CardDescription>
          </div>
          <div className="flex gap-2">
            <Button type="button" variant="outline" onClick={addDraft} disabled={isPending}>
              <Plus className="h-4 w-4" />
              添加 Key
            </Button>
            <Button type="button" onClick={handleSave} disabled={isPending}>
              <Save className="h-4 w-4" />
              保存
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="rounded-md border p-3">
          <div className="flex flex-col gap-3 lg:flex-row lg:items-center">
            <div className="flex min-w-[160px] items-center gap-2">
              <Badge variant={keepLegacy ? 'secondary' : 'outline'}>legacy</Badge>
              {data?.legacyKeyMask && (
                <span className="font-mono text-xs text-muted-foreground">
                  {data.legacyKeyMask}
                </span>
              )}
            </div>
            <Input
              value={legacyApiKey}
              onChange={(event) => setLegacyApiKey(event.target.value)}
              placeholder={keepLegacy ? '留空保留当前 legacy key' : '填写后启用全量访问 key'}
              disabled={isPending || !keepLegacy}
              type="password"
              autoComplete="new-password"
            />
            <Button
              type="button"
              variant={keepLegacy ? 'secondary' : 'outline'}
              onClick={() => setKeepLegacy((value) => !value)}
              disabled={isPending}
            >
              <KeyRound className="h-4 w-4" />
              {keepLegacy ? '保留' : '启用'}
            </Button>
          </div>
        </div>

        <div className="space-y-3">
          {drafts.map((draft, index) => (
            <div key={`${draft.id}-${index}`} className="rounded-md border p-3">
              <div className="grid gap-3 lg:grid-cols-[minmax(120px,180px)_minmax(180px,1fr)_minmax(260px,1.5fr)_auto] lg:items-start">
                <Input
                  value={draft.id}
                  onChange={(event) => updateDraft(index, { id: event.target.value })}
                  placeholder="key-id"
                  disabled={isPending}
                />
                <Input
                  value={draft.key}
                  onChange={(event) => updateDraft(index, { key: event.target.value })}
                  placeholder={draft.isNew ? '客户端 API Key' : `留空保留 ${draft.keyMask}`}
                  disabled={isPending}
                  type="password"
                  autoComplete="new-password"
                />
                <CredentialGroupPicker
                  value={draft.allowedCredentialGroups}
                  onChange={(value) => updateDraft(index, { allowedCredentialGroups: value })}
                  disabled={isPending}
                  compact
                />
                <Button
                  type="button"
                  variant="outline"
                  size="icon"
                  onClick={() => removeDraft(index)}
                  disabled={isPending}
                  aria-label="删除 API Key"
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
            </div>
          ))}
          {drafts.length === 0 && (
            <div className="rounded-md border border-dashed p-4 text-sm text-muted-foreground">
              当前没有 scoped API Key
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
