import { useEffect, useMemo, useState } from 'react'
import { Plus, Save, Trash2 } from 'lucide-react'
import { toast } from 'sonner'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import {
  useCredentialGroupsConfig,
  useSetCredentialGroupsConfig,
} from '@/hooks/use-credentials'
import { normalizeCredentialGroups } from '@/lib/credential-groups'
import { extractErrorMessage } from '@/lib/utils'
import type { CredentialGroupConfigItem } from '@/types/api'

function normalizeGroupName(value: string): string {
  return normalizeCredentialGroups(value)[0] ?? ''
}

function emptyToNull(value: string): string | null {
  const trimmed = value.trim()
  return trimmed ? trimmed : null
}

export function CredentialGroupsSettings() {
  const { data, isLoading } = useCredentialGroupsConfig()
  const { mutate: saveGroups, isPending } = useSetCredentialGroupsConfig()
  const [groups, setGroups] = useState<CredentialGroupConfigItem[]>([])
  const [newName, setNewName] = useState('')
  const [newDisplayName, setNewDisplayName] = useState('')
  const [newDescription, setNewDescription] = useState('')

  useEffect(() => {
    if (data?.groups) {
      setGroups(data.groups)
    }
  }, [data?.groups])

  const usageByName = useMemo(
    () => new Map((data?.usage ?? []).map((item) => [item.name, item])),
    [data?.usage]
  )
  const unknownGroups = data?.unknownCredentialGroups ?? []

  const updateGroup = (name: string, patch: Partial<CredentialGroupConfigItem>) => {
    setGroups((current) =>
      current.map((group) => (group.name === name ? { ...group, ...patch } : group))
    )
  }

  const addGroup = () => {
    const name = normalizeGroupName(newName)
    if (!name) {
      toast.error('分组名称不能为空')
      return
    }
    if (groups.some((group) => group.name === name)) {
      toast.error(`分组已存在: ${name}`)
      return
    }

    setGroups((current) =>
      [
        ...current,
        {
          name,
          displayName: emptyToNull(newDisplayName) ?? name,
          description: emptyToNull(newDescription),
          enabled: true,
        },
      ].sort((a, b) => a.name.localeCompare(b.name))
    )
    setNewName('')
    setNewDisplayName('')
    setNewDescription('')
  }

  const removeGroup = (name: string) => {
    setGroups((current) => current.filter((group) => group.name !== name))
  }

  const handleSave = () => {
    saveGroups(
      {
        groups: groups.map((group) => ({
          name: group.name,
          displayName: emptyToNull(group.displayName ?? ''),
          description: emptyToNull(group.description ?? ''),
          enabled: group.name === 'default' ? true : group.enabled,
        })),
      },
      {
        onSuccess: (response) => {
          setGroups(response.groups)
          toast.success('凭据分组目录已更新')
        },
        onError: (error) => {
          toast.error(`保存失败: ${extractErrorMessage(error)}`)
        },
      }
    )
  }

  if (isLoading) {
    return (
      <Card className="border-muted shadow-sm">
        <CardHeader>
          <CardTitle>凭据分组目录</CardTitle>
          <CardDescription>正在加载分组目录</CardDescription>
        </CardHeader>
      </Card>
    )
  }

  return (
    <Card className="border-muted shadow-sm">
      <CardHeader>
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <CardTitle>凭据分组目录</CardTitle>
            <CardDescription>统一管理可用于凭据和导入批次的分组标记</CardDescription>
          </div>
          <Button type="button" onClick={handleSave} disabled={isPending}>
            <Save className="h-4 w-4" />
            保存目录
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {unknownGroups.length > 0 && (
          <div className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-800">
            现有凭据包含未登记分组: {unknownGroups.join(', ')}
          </div>
        )}

        <div className="grid gap-3 rounded-md border p-3 lg:grid-cols-[minmax(120px,1fr)_minmax(140px,1fr)_minmax(180px,2fr)_auto]">
          <Input
            placeholder="分组名称"
            value={newName}
            onChange={(event) => setNewName(event.target.value)}
            disabled={isPending}
          />
          <Input
            placeholder="显示名"
            value={newDisplayName}
            onChange={(event) => setNewDisplayName(event.target.value)}
            disabled={isPending}
          />
          <Input
            placeholder="描述"
            value={newDescription}
            onChange={(event) => setNewDescription(event.target.value)}
            disabled={isPending}
          />
          <Button type="button" variant="outline" onClick={addGroup} disabled={isPending}>
            <Plus className="h-4 w-4" />
            添加
          </Button>
        </div>

        <div className="space-y-3">
          {groups.map((group) => {
            const usage = usageByName.get(group.name)
            const locked =
              group.name === 'default' ||
              (usage?.credentialCount ?? 0) > 0 ||
              (usage?.apiKeyCount ?? 0) > 0
            return (
              <div key={group.name} className="rounded-md border p-3">
                <div className="flex flex-col gap-3 lg:flex-row lg:items-center">
                  <div className="flex min-w-[120px] items-center gap-2">
                    <Badge variant={group.enabled ? 'secondary' : 'outline'}>
                      {group.name}
                    </Badge>
                    {usage && usage.apiKeyCount === 0 && !data?.legacyFullAccessKey && (
                      <Badge variant="warning">无 Key</Badge>
                    )}
                  </div>
                  <Input
                    value={group.displayName ?? ''}
                    onChange={(event) =>
                      updateGroup(group.name, { displayName: event.target.value })
                    }
                    placeholder={group.name}
                    disabled={isPending}
                  />
                  <Input
                    value={group.description ?? ''}
                    onChange={(event) =>
                      updateGroup(group.name, { description: event.target.value })
                    }
                    placeholder="描述"
                    disabled={isPending}
                  />
                  <div className="flex shrink-0 items-center gap-3">
                    <div className="text-xs text-muted-foreground">
                      凭据 {usage?.credentialCount ?? 0} / Key {usage?.apiKeyCount ?? 0}
                    </div>
                    <Switch
                      checked={group.name === 'default' ? true : group.enabled}
                      onCheckedChange={(checked) =>
                        updateGroup(group.name, { enabled: Boolean(checked) })
                      }
                      disabled={isPending || group.name === 'default'}
                    />
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      onClick={() => removeGroup(group.name)}
                      disabled={isPending || locked}
                      aria-label={`删除分组 ${group.name}`}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
              </div>
            )
          })}
        </div>
      </CardContent>
    </Card>
  )
}
