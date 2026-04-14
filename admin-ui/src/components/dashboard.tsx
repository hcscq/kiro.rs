import { useState, useEffect, useRef } from 'react'
import { RefreshCw, LogOut, Moon, Sun, Server, Plus, Upload, FileUp, Trash2, RotateCcw, CheckCircle2 } from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { storage } from '@/lib/storage'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { CredentialCard } from '@/components/credential-card'
import { BalanceDialog } from '@/components/balance-dialog'
import { AddCredentialDialog } from '@/components/add-credential-dialog'
import { BatchImportDialog } from '@/components/batch-import-dialog'
import { KamImportDialog } from '@/components/kam-import-dialog'
import { BatchVerifyDialog, type VerifyResult } from '@/components/batch-verify-dialog'
import { useCredentials, useDeleteCredential, useResetFailure, useLoadBalancingMode, useSetLoadBalancingMode } from '@/hooks/use-credentials'
import { getCredentialBalance, forceRefreshToken } from '@/api/credentials'
import { cn, extractErrorMessage } from '@/lib/utils'
import type { BalanceResponse, CredentialStatusItem } from '@/types/api'

interface DashboardProps {
  onLogout: () => void
}

const ALL_LEVELS = '__all_levels__'
const UNKNOWN_LEVEL = '__unknown_level__'

type EnabledFilter = 'all' | 'enabled' | 'disabled'
type AccountStatusFilter = 'all' | 'normal' | 'rate-limited' | 'abnormal'
type QuickFilter = 'all' | 'dispatchable'
type SortField = 'importedAt' | 'priority' | 'successCount' | 'lastUsedAt'
type SortDirection = 'asc' | 'desc'

interface SegmentedOption {
  value: string
  label: string
}

interface SegmentedTabsProps {
  label: string
  options: SegmentedOption[]
  value: string
  onChange: (value: string) => void
}

function SegmentedTabs({ label, options, value, onChange }: SegmentedTabsProps) {
  return (
    <div className="space-y-2">
      <div className="text-sm font-medium">{label}</div>
      <div className="flex flex-wrap gap-2">
        {options.map((option) => {
          const active = option.value === value

          return (
            <Button
              key={option.value}
              type="button"
              size="sm"
              variant={active ? 'default' : 'outline'}
              className={cn('h-8 rounded-full px-3', !active && 'text-muted-foreground')}
              onClick={() => onChange(option.value)}
            >
              {option.label}
            </Button>
          )
        })}
      </div>
    </div>
  )
}

function normalizeSubscriptionTitle(credential: CredentialStatusItem): string | null {
  const title = credential.subscriptionTitle?.trim()
  return title ? title : null
}

function isRateLimitedCredential(credential: CredentialStatusItem): boolean {
  return (
    (credential.cooldownRemainingMs ?? 0) > 0 ||
    credential.rateLimitHitStreak > 0 ||
    (credential.nextReadyInMs ?? 0) > 0
  )
}

function isAbnormalCredential(credential: CredentialStatusItem): boolean {
  return (
    credential.failureCount > 0 ||
    credential.refreshFailureCount > 0 ||
    (credential.disabledReason !== undefined &&
      credential.disabledReason !== null &&
      credential.disabledReason !== 'Manual')
  )
}

function isDispatchableCredential(credential: CredentialStatusItem): boolean {
  const hasCapacity =
    credential.maxConcurrency === undefined ||
    credential.maxConcurrency === null ||
    credential.inFlight < credential.maxConcurrency

  return !credential.disabled && !isRateLimitedCredential(credential) && hasCapacity
}

function getImportedAtSortValue(credential: CredentialStatusItem): number {
  if (credential.importedAt) {
    const timestamp = Date.parse(credential.importedAt)
    if (!Number.isNaN(timestamp)) {
      return timestamp
    }
  }

  return credential.id
}

function getLastUsedSortValue(credential: CredentialStatusItem): number {
  if (!credential.lastUsedAt) {
    return -1
  }

  const timestamp = Date.parse(credential.lastUsedAt)
  return Number.isNaN(timestamp) ? -1 : timestamp
}

function getDefaultSortDirection(field: SortField): SortDirection {
  return field === 'priority' ? 'asc' : 'desc'
}

export function Dashboard({ onLogout }: DashboardProps) {
  const [selectedCredentialId, setSelectedCredentialId] = useState<number | null>(null)
  const [balanceDialogOpen, setBalanceDialogOpen] = useState(false)
  const [addDialogOpen, setAddDialogOpen] = useState(false)
  const [batchImportDialogOpen, setBatchImportDialogOpen] = useState(false)
  const [kamImportDialogOpen, setKamImportDialogOpen] = useState(false)
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set())
  const [verifyDialogOpen, setVerifyDialogOpen] = useState(false)
  const [verifying, setVerifying] = useState(false)
  const [verifyProgress, setVerifyProgress] = useState({ current: 0, total: 0 })
  const [verifyResults, setVerifyResults] = useState<Map<number, VerifyResult>>(new Map())
  const [balanceMap, setBalanceMap] = useState<Map<number, BalanceResponse>>(new Map())
  const [loadingBalanceIds, setLoadingBalanceIds] = useState<Set<number>>(new Set())
  const [queryingInfo, setQueryingInfo] = useState(false)
  const [queryInfoProgress, setQueryInfoProgress] = useState({ current: 0, total: 0 })
  const [batchRefreshing, setBatchRefreshing] = useState(false)
  const [batchRefreshProgress, setBatchRefreshProgress] = useState({ current: 0, total: 0 })
  const [queueMaxSizeInput, setQueueMaxSizeInput] = useState('0')
  const [queueMaxWaitMsInput, setQueueMaxWaitMsInput] = useState('0')
  const [rateLimitCooldownMsInput, setRateLimitCooldownMsInput] = useState('2000')
  const [defaultMaxConcurrencyInput, setDefaultMaxConcurrencyInput] = useState('')
  const [rateLimitBucketCapacityInput, setRateLimitBucketCapacityInput] = useState('3')
  const [rateLimitRefillPerSecondInput, setRateLimitRefillPerSecondInput] = useState('1')
  const [rateLimitRefillMinPerSecondInput, setRateLimitRefillMinPerSecondInput] = useState('0.2')
  const [rateLimitRefillRecoveryStepInput, setRateLimitRefillRecoveryStepInput] = useState('0.1')
  const [rateLimitRefillBackoffFactorInput, setRateLimitRefillBackoffFactorInput] = useState('0.5')
  const cancelVerifyRef = useRef(false)
  const [currentPage, setCurrentPage] = useState(1)
  const [levelFilter, setLevelFilter] = useState<string>(ALL_LEVELS)
  const [enabledFilter, setEnabledFilter] = useState<EnabledFilter>('all')
  const [accountStatusFilter, setAccountStatusFilter] = useState<AccountStatusFilter>('all')
  const [quickFilter, setQuickFilter] = useState<QuickFilter>('all')
  const [searchKeyword, setSearchKeyword] = useState('')
  const [sortField, setSortField] = useState<SortField>('importedAt')
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc')
  const itemsPerPage = 12
  const [darkMode, setDarkMode] = useState(() => {
    if (typeof window !== 'undefined') {
      return document.documentElement.classList.contains('dark')
    }
    return false
  })

  const queryClient = useQueryClient()
  const { data, isLoading, error, refetch } = useCredentials()
  const { mutate: deleteCredential } = useDeleteCredential()
  const { mutate: resetFailure } = useResetFailure()
  const { data: loadBalancingData, isLoading: isLoadingMode } = useLoadBalancingMode()
  const { mutate: setLoadBalancingMode, isPending: isSettingMode } = useSetLoadBalancingMode()

  const credentials = data?.credentials || []
  const levelOptions = [
    { value: ALL_LEVELS, label: '全部' },
    ...Array.from(new Set(credentials.map(credential => normalizeSubscriptionTitle(credential) ?? UNKNOWN_LEVEL)))
      .sort((a, b) => {
        if (a === UNKNOWN_LEVEL) return 1
        if (b === UNKNOWN_LEVEL) return -1
        return a.localeCompare(b, 'zh-CN')
      })
      .map(level => ({
        value: level,
        label: level === UNKNOWN_LEVEL ? '未知' : level,
      })),
  ]

  const enabledOptions: SegmentedOption[] = [
    { value: 'all', label: '全部' },
    { value: 'enabled', label: '启用中' },
    { value: 'disabled', label: '已禁用' },
  ]

  const accountStatusOptions: SegmentedOption[] = [
    { value: 'all', label: '全部' },
    { value: 'normal', label: '正常' },
    { value: 'rate-limited', label: '限速' },
    { value: 'abnormal', label: '异常' },
  ]

  const sortOptions: SegmentedOption[] = [
    { value: 'importedAt', label: '导入时间' },
    { value: 'priority', label: '优先级' },
    { value: 'successCount', label: '调用次数' },
    { value: 'lastUsedAt', label: '最后调用' },
  ]

  const keyword = searchKeyword.trim().toLowerCase()
  const filteredCredentials = [...credentials]
    .filter((credential) => {
      const normalizedLevel = normalizeSubscriptionTitle(credential)

      if (levelFilter === UNKNOWN_LEVEL && normalizedLevel !== null) {
        return false
      }
      if (levelFilter !== ALL_LEVELS && levelFilter !== UNKNOWN_LEVEL && normalizedLevel !== levelFilter) {
        return false
      }

      if (enabledFilter === 'enabled' && credential.disabled) {
        return false
      }
      if (enabledFilter === 'disabled' && !credential.disabled) {
        return false
      }

      if (accountStatusFilter === 'normal' && (isRateLimitedCredential(credential) || isAbnormalCredential(credential))) {
        return false
      }
      if (accountStatusFilter === 'rate-limited' && !isRateLimitedCredential(credential)) {
        return false
      }
      if (accountStatusFilter === 'abnormal' && !isAbnormalCredential(credential)) {
        return false
      }

      if (quickFilter === 'dispatchable' && !isDispatchableCredential(credential)) {
        return false
      }

      if (keyword) {
        const searchableText = [
          credential.email,
          credential.id.toString(),
          normalizeSubscriptionTitle(credential),
          credential.proxyUrl,
          credential.disabledReason,
        ]
          .filter(Boolean)
          .join(' ')
          .toLowerCase()

        if (!searchableText.includes(keyword)) {
          return false
        }
      }

      return true
    })
    .sort((a, b) => {
      let comparison = 0

      if (sortField === 'importedAt') {
        comparison = getImportedAtSortValue(a) - getImportedAtSortValue(b)
      } else if (sortField === 'priority') {
        comparison = a.priority - b.priority
      } else if (sortField === 'successCount') {
        comparison = a.successCount - b.successCount
      } else if (sortField === 'lastUsedAt') {
        comparison = getLastUsedSortValue(a) - getLastUsedSortValue(b)
      }

      if (comparison === 0) {
        return b.id - a.id
      }

      return sortDirection === 'asc' ? comparison : -comparison
    })

  // 计算分页
  const totalPages = filteredCredentials.length === 0 ? 0 : Math.ceil(filteredCredentials.length / itemsPerPage)
  const effectiveCurrentPage = totalPages === 0 ? 1 : Math.min(currentPage, totalPages)
  const startIndex = (effectiveCurrentPage - 1) * itemsPerPage
  const endIndex = startIndex + itemsPerPage
  const currentCredentials = filteredCredentials.slice(startIndex, endIndex)
  const disabledCredentialCount = credentials.filter(credential => credential.disabled).length
  const selectedDisabledCount = Array.from(selectedIds).filter(id => {
    const credential = credentials.find(c => c.id === id)
    return Boolean(credential?.disabled)
  }).length

  // 当筛选或排序变化时重置到第一页
  useEffect(() => {
    setCurrentPage(1)
  }, [data?.credentials.length, levelFilter, enabledFilter, accountStatusFilter, quickFilter, searchKeyword, sortField, sortDirection])

  // 过滤结果变少时自动修正页码
  useEffect(() => {
    if (totalPages === 0 && currentPage !== 1) {
      setCurrentPage(1)
      return
    }

    if (totalPages > 0 && currentPage > totalPages) {
      setCurrentPage(totalPages)
    }
  }, [currentPage, totalPages])

  useEffect(() => {
    if (!loadBalancingData) {
      return
    }
    setQueueMaxSizeInput(String(loadBalancingData.queueMaxSize))
    setQueueMaxWaitMsInput(String(loadBalancingData.queueMaxWaitMs))
    setRateLimitCooldownMsInput(String(loadBalancingData.rateLimitCooldownMs))
    setDefaultMaxConcurrencyInput(loadBalancingData.defaultMaxConcurrency ? String(loadBalancingData.defaultMaxConcurrency) : '')
    setRateLimitBucketCapacityInput(String(loadBalancingData.rateLimitBucketCapacity))
    setRateLimitRefillPerSecondInput(String(loadBalancingData.rateLimitRefillPerSecond))
    setRateLimitRefillMinPerSecondInput(String(loadBalancingData.rateLimitRefillMinPerSecond))
    setRateLimitRefillRecoveryStepInput(String(loadBalancingData.rateLimitRefillRecoveryStepPerSuccess))
    setRateLimitRefillBackoffFactorInput(String(loadBalancingData.rateLimitRefillBackoffFactor))
  }, [
    loadBalancingData?.queueMaxSize,
    loadBalancingData?.queueMaxWaitMs,
    loadBalancingData?.rateLimitCooldownMs,
    loadBalancingData?.defaultMaxConcurrency,
    loadBalancingData?.rateLimitBucketCapacity,
    loadBalancingData?.rateLimitRefillPerSecond,
    loadBalancingData?.rateLimitRefillMinPerSecond,
    loadBalancingData?.rateLimitRefillRecoveryStepPerSuccess,
    loadBalancingData?.rateLimitRefillBackoffFactor,
  ])

  // 只保留当前仍存在的凭据缓存，避免删除后残留旧数据
  useEffect(() => {
    if (!data?.credentials) {
      setBalanceMap(new Map())
      setLoadingBalanceIds(new Set())
      return
    }

    const validIds = new Set(data.credentials.map(credential => credential.id))

    setBalanceMap(prev => {
      const next = new Map<number, BalanceResponse>()
      prev.forEach((value, id) => {
        if (validIds.has(id)) {
          next.set(id, value)
        }
      })
      return next.size === prev.size ? prev : next
    })

    setLoadingBalanceIds(prev => {
      if (prev.size === 0) {
        return prev
      }
      const next = new Set<number>()
      prev.forEach(id => {
        if (validIds.has(id)) {
          next.add(id)
        }
      })
      return next.size === prev.size ? prev : next
    })
  }, [data?.credentials])

  const toggleDarkMode = () => {
    setDarkMode(!darkMode)
    document.documentElement.classList.toggle('dark')
  }

  const handleViewBalance = async (id: number) => {
    setSelectedCredentialId(id)
    setBalanceDialogOpen(true)

    if (balanceMap.has(id) || loadingBalanceIds.has(id)) {
      return
    }

    setLoadingBalanceIds(prev => {
      const next = new Set(prev)
      next.add(id)
      return next
    })

    try {
      const balance = await getCredentialBalance(id)
      setBalanceMap(prev => {
        const next = new Map(prev)
        next.set(id, balance)
        return next
      })
    } catch {
      // 弹窗会展示独立查询错误，这里静默跳过卡片回填失败
    } finally {
      setLoadingBalanceIds(prev => {
        const next = new Set(prev)
        next.delete(id)
        return next
      })
    }
  }

  const handleRefresh = () => {
    refetch()
    toast.success('已刷新凭据列表')
  }

  const resetFilters = () => {
    setLevelFilter(ALL_LEVELS)
    setEnabledFilter('all')
    setAccountStatusFilter('all')
    setQuickFilter('all')
    setSearchKeyword('')
    setSortField('importedAt')
    setSortDirection('desc')
  }

  const handleLogout = () => {
    storage.removeApiKey()
    queryClient.clear()
    onLogout()
  }

  // 选择管理
  const toggleSelect = (id: number) => {
    const newSelected = new Set(selectedIds)
    if (newSelected.has(id)) {
      newSelected.delete(id)
    } else {
      newSelected.add(id)
    }
    setSelectedIds(newSelected)
  }

  const deselectAll = () => {
    setSelectedIds(new Set())
  }

  // 批量删除（仅删除已禁用项）
  const handleBatchDelete = async () => {
    if (selectedIds.size === 0) {
      toast.error('请先选择要删除的凭据')
      return
    }

    const disabledIds = Array.from(selectedIds).filter(id => {
      const credential = data?.credentials.find(c => c.id === id)
      return Boolean(credential?.disabled)
    })

    if (disabledIds.length === 0) {
      toast.error('选中的凭据中没有已禁用项')
      return
    }

    const skippedCount = selectedIds.size - disabledIds.length
    const skippedText = skippedCount > 0 ? `（将跳过 ${skippedCount} 个未禁用凭据）` : ''

    if (!confirm(`确定要删除 ${disabledIds.length} 个已禁用凭据吗？此操作无法撤销。${skippedText}`)) {
      return
    }

    let successCount = 0
    let failCount = 0

    for (const id of disabledIds) {
      try {
        await new Promise<void>((resolve, reject) => {
          deleteCredential(id, {
            onSuccess: () => {
              successCount++
              resolve()
            },
            onError: (err) => {
              failCount++
              reject(err)
            }
          })
        })
      } catch (error) {
        // 错误已在 onError 中处理
      }
    }

    const skippedResultText = skippedCount > 0 ? `，已跳过 ${skippedCount} 个未禁用凭据` : ''

    if (failCount === 0) {
      toast.success(`成功删除 ${successCount} 个已禁用凭据${skippedResultText}`)
    } else {
      toast.warning(`删除已禁用凭据：成功 ${successCount} 个，失败 ${failCount} 个${skippedResultText}`)
    }

    deselectAll()
  }

  // 批量恢复异常
  const handleBatchResetFailure = async () => {
    if (selectedIds.size === 0) {
      toast.error('请先选择要恢复的凭据')
      return
    }

    const failedIds = Array.from(selectedIds).filter(id => {
      const cred = data?.credentials.find(c => c.id === id)
      return cred && cred.failureCount > 0
    })

    if (failedIds.length === 0) {
      toast.error('选中的凭据中没有失败的凭据')
      return
    }

    let successCount = 0
    let failCount = 0

    for (const id of failedIds) {
      try {
        await new Promise<void>((resolve, reject) => {
          resetFailure(id, {
            onSuccess: () => {
              successCount++
              resolve()
            },
            onError: (err) => {
              failCount++
              reject(err)
            }
          })
        })
      } catch (error) {
        // 错误已在 onError 中处理
      }
    }

    if (failCount === 0) {
      toast.success(`成功恢复 ${successCount} 个凭据`)
    } else {
      toast.warning(`成功 ${successCount} 个，失败 ${failCount} 个`)
    }

    deselectAll()
  }

  // 批量刷新 Token
  const handleBatchForceRefresh = async () => {
    if (selectedIds.size === 0) {
      toast.error('请先选择要刷新的凭据')
      return
    }

    const enabledIds = Array.from(selectedIds).filter(id => {
      const cred = data?.credentials.find(c => c.id === id)
      return cred && !cred.disabled
    })

    if (enabledIds.length === 0) {
      toast.error('选中的凭据中没有启用的凭据')
      return
    }

    setBatchRefreshing(true)
    setBatchRefreshProgress({ current: 0, total: enabledIds.length })

    let successCount = 0
    let failCount = 0

    for (let i = 0; i < enabledIds.length; i++) {
      try {
        await forceRefreshToken(enabledIds[i])
        successCount++
      } catch {
        failCount++
      }
      setBatchRefreshProgress({ current: i + 1, total: enabledIds.length })
    }

    setBatchRefreshing(false)
    queryClient.invalidateQueries({ queryKey: ['credentials'] })

    if (failCount === 0) {
      toast.success(`成功刷新 ${successCount} 个凭据的 Token`)
    } else {
      toast.warning(`刷新 Token：成功 ${successCount} 个，失败 ${failCount} 个`)
    }

    deselectAll()
  }

  // 一键清除所有已禁用凭据
  const handleClearAll = async () => {
    if (!data?.credentials || data.credentials.length === 0) {
      toast.error('没有可清除的凭据')
      return
    }

    const disabledCredentials = data.credentials.filter(credential => credential.disabled)

    if (disabledCredentials.length === 0) {
      toast.error('没有可清除的已禁用凭据')
      return
    }

    if (!confirm(`确定要清除所有 ${disabledCredentials.length} 个已禁用凭据吗？此操作无法撤销。`)) {
      return
    }

    let successCount = 0
    let failCount = 0

    for (const credential of disabledCredentials) {
      try {
        await new Promise<void>((resolve, reject) => {
          deleteCredential(credential.id, {
            onSuccess: () => {
              successCount++
              resolve()
            },
            onError: (err) => {
              failCount++
              reject(err)
            }
          })
        })
      } catch (error) {
        // 错误已在 onError 中处理
      }
    }

    if (failCount === 0) {
      toast.success(`成功清除所有 ${successCount} 个已禁用凭据`)
    } else {
      toast.warning(`清除已禁用凭据：成功 ${successCount} 个，失败 ${failCount} 个`)
    }

    deselectAll()
  }

  // 查询当前页凭据信息（逐个查询，避免瞬时并发）
  const handleQueryCurrentPageInfo = async () => {
    if (currentCredentials.length === 0) {
      toast.error('当前页没有可查询的凭据')
      return
    }

    const ids = currentCredentials
      .filter(credential => !credential.disabled)
      .map(credential => credential.id)

    if (ids.length === 0) {
      toast.error('当前页没有可查询的启用凭据')
      return
    }

    setQueryingInfo(true)
    setQueryInfoProgress({ current: 0, total: ids.length })

    let successCount = 0
    let failCount = 0

    for (let i = 0; i < ids.length; i++) {
      const id = ids[i]

      setLoadingBalanceIds(prev => {
        const next = new Set(prev)
        next.add(id)
        return next
      })

      try {
        const balance = await getCredentialBalance(id)
        successCount++

        setBalanceMap(prev => {
          const next = new Map(prev)
          next.set(id, balance)
          return next
        })
      } catch (error) {
        failCount++
      } finally {
        setLoadingBalanceIds(prev => {
          const next = new Set(prev)
          next.delete(id)
          return next
        })
      }

      setQueryInfoProgress({ current: i + 1, total: ids.length })
    }

    setQueryingInfo(false)

    if (failCount === 0) {
      toast.success(`查询完成：成功 ${successCount}/${ids.length}`)
    } else {
      toast.warning(`查询完成：成功 ${successCount} 个，失败 ${failCount} 个`)
    }
  }

  // 批量验活
  const handleBatchVerify = async () => {
    if (selectedIds.size === 0) {
      toast.error('请先选择要验活的凭据')
      return
    }

    // 初始化状态
    setVerifying(true)
    cancelVerifyRef.current = false
    const ids = Array.from(selectedIds)
    setVerifyProgress({ current: 0, total: ids.length })

    let successCount = 0

    // 初始化结果，所有凭据状态为 pending
    const initialResults = new Map<number, VerifyResult>()
    ids.forEach(id => {
      initialResults.set(id, { id, status: 'pending' })
    })
    setVerifyResults(initialResults)
    setVerifyDialogOpen(true)

    // 开始验活
    for (let i = 0; i < ids.length; i++) {
      // 检查是否取消
      if (cancelVerifyRef.current) {
        toast.info('已取消验活')
        break
      }

      const id = ids[i]

      // 更新当前凭据状态为 verifying
      setVerifyResults(prev => {
        const newResults = new Map(prev)
        newResults.set(id, { id, status: 'verifying' })
        return newResults
      })

      try {
        const balance = await getCredentialBalance(id)
        successCount++

        // 更新为成功状态
        setVerifyResults(prev => {
          const newResults = new Map(prev)
          newResults.set(id, {
            id,
            status: 'success',
            usage: `${balance.currentUsage}/${balance.usageLimit}`
          })
          return newResults
        })
      } catch (error) {
        // 更新为失败状态
        setVerifyResults(prev => {
          const newResults = new Map(prev)
          newResults.set(id, {
            id,
            status: 'failed',
            error: extractErrorMessage(error)
          })
          return newResults
        })
      }

      // 更新进度
      setVerifyProgress({ current: i + 1, total: ids.length })

      // 添加延迟防止封号（最后一个不需要延迟）
      if (i < ids.length - 1 && !cancelVerifyRef.current) {
        await new Promise(resolve => setTimeout(resolve, 2000))
      }
    }

    setVerifying(false)

    if (!cancelVerifyRef.current) {
      toast.success(`验活完成：成功 ${successCount}/${ids.length}`)
    }
  }

  // 取消验活
  const handleCancelVerify = () => {
    cancelVerifyRef.current = true
    setVerifying(false)
  }

  // 切换负载均衡模式
  const handleToggleLoadBalancing = () => {
    const currentMode = loadBalancingData?.mode || 'priority'
    const newMode = currentMode === 'priority' ? 'balanced' : 'priority'

    setLoadBalancingMode({ mode: newMode }, {
      onSuccess: () => {
        const modeName = newMode === 'priority' ? '优先级模式' : '均衡负载模式'
        toast.success(`已切换到${modeName}`)
      },
      onError: (error) => {
        toast.error(`切换失败: ${extractErrorMessage(error)}`)
      }
    })
  }

  const handleSortFieldChange = (value: string) => {
    const nextField = value as SortField
    setSortField(nextField)
    setSortDirection(getDefaultSortDirection(nextField))
  }

  const handleSaveQueueSettings = () => {
    const parsedQueueMaxSize = queueMaxSizeInput.trim() === '' ? 0 : parseInt(queueMaxSizeInput, 10)
    const parsedQueueMaxWaitMs = queueMaxWaitMsInput.trim() === '' ? 0 : parseInt(queueMaxWaitMsInput, 10)
    const parsedRateLimitCooldownMs = rateLimitCooldownMsInput.trim() === '' ? 0 : parseInt(rateLimitCooldownMsInput, 10)
    const parsedDefaultMaxConcurrency = defaultMaxConcurrencyInput.trim() === '' ? 0 : parseInt(defaultMaxConcurrencyInput, 10)
    const parsedRateLimitBucketCapacity = rateLimitBucketCapacityInput.trim() === '' ? 0 : Number.parseFloat(rateLimitBucketCapacityInput)
    const parsedRateLimitRefillPerSecond = rateLimitRefillPerSecondInput.trim() === '' ? 0 : Number.parseFloat(rateLimitRefillPerSecondInput)
    const parsedRateLimitRefillMinPerSecond = rateLimitRefillMinPerSecondInput.trim() === '' ? 0 : Number.parseFloat(rateLimitRefillMinPerSecondInput)
    const parsedRateLimitRefillRecoveryStep = rateLimitRefillRecoveryStepInput.trim() === '' ? 0 : Number.parseFloat(rateLimitRefillRecoveryStepInput)
    const parsedRateLimitRefillBackoffFactor = rateLimitRefillBackoffFactorInput.trim() === '' ? 0 : Number.parseFloat(rateLimitRefillBackoffFactorInput)

    if (
      Number.isNaN(parsedQueueMaxSize) ||
      Number.isNaN(parsedQueueMaxWaitMs) ||
      Number.isNaN(parsedRateLimitCooldownMs) ||
      Number.isNaN(parsedDefaultMaxConcurrency) ||
      Number.isNaN(parsedRateLimitBucketCapacity) ||
      Number.isNaN(parsedRateLimitRefillPerSecond) ||
      Number.isNaN(parsedRateLimitRefillMinPerSecond) ||
      Number.isNaN(parsedRateLimitRefillRecoveryStep) ||
      Number.isNaN(parsedRateLimitRefillBackoffFactor) ||
      parsedQueueMaxSize < 0 ||
      parsedQueueMaxWaitMs < 0 ||
      parsedRateLimitCooldownMs < 0 ||
      parsedDefaultMaxConcurrency < 0 ||
      parsedRateLimitBucketCapacity < 0 ||
      parsedRateLimitRefillPerSecond < 0 ||
      parsedRateLimitRefillMinPerSecond < 0 ||
      parsedRateLimitRefillRecoveryStep < 0
    ) {
      toast.error('调度参数必须是大于等于 0 的数字')
      return
    }

    if (
      parsedRateLimitRefillBackoffFactor < 0.05 ||
      parsedRateLimitRefillBackoffFactor > 1
    ) {
      toast.error('429 衰减系数必须在 0.05 到 1 之间')
      return
    }

    if (
      parsedRateLimitRefillPerSecond > 0 &&
      parsedRateLimitRefillMinPerSecond > parsedRateLimitRefillPerSecond
    ) {
      toast.error('最小回填速率不能大于基础回填速率')
      return
    }

    setLoadBalancingMode(
      {
        queueMaxSize: parsedQueueMaxSize,
        queueMaxWaitMs: parsedQueueMaxWaitMs,
        rateLimitCooldownMs: parsedRateLimitCooldownMs,
        defaultMaxConcurrency: parsedDefaultMaxConcurrency,
        rateLimitBucketCapacity: parsedRateLimitBucketCapacity,
        rateLimitRefillPerSecond: parsedRateLimitRefillPerSecond,
        rateLimitRefillMinPerSecond: parsedRateLimitRefillMinPerSecond,
        rateLimitRefillRecoveryStepPerSuccess: parsedRateLimitRefillRecoveryStep,
        rateLimitRefillBackoffFactor: parsedRateLimitRefillBackoffFactor,
      },
      {
        onSuccess: () => {
          toast.success('调度配置已更新')
        },
        onError: (error) => {
          toast.error(`保存失败: ${extractErrorMessage(error)}`)
        }
      }
    )
  }

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary mx-auto mb-4"></div>
          <p className="text-muted-foreground">加载中...</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background p-4">
        <Card className="w-full max-w-md">
          <CardContent className="pt-6 text-center">
            <div className="text-red-500 mb-4">加载失败</div>
            <p className="text-muted-foreground mb-4">{(error as Error).message}</p>
            <div className="space-x-2">
              <Button onClick={() => refetch()}>重试</Button>
              <Button variant="outline" onClick={handleLogout}>重新登录</Button>
            </div>
          </CardContent>
        </Card>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-background">
      {/* 顶部导航 */}
      <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
        <div className="container flex h-14 items-center justify-between px-4 md:px-8">
          <div className="flex items-center gap-2">
            <Server className="h-5 w-5" />
            <span className="font-semibold">Kiro Admin</span>
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={handleToggleLoadBalancing}
              disabled={isLoadingMode || isSettingMode}
              title="切换负载均衡模式"
            >
              {isLoadingMode ? '加载中...' : (loadBalancingData?.mode === 'priority' ? '优先级模式' : '均衡负载')}
            </Button>
            <Button variant="ghost" size="icon" onClick={toggleDarkMode}>
              {darkMode ? <Sun className="h-5 w-5" /> : <Moon className="h-5 w-5" />}
            </Button>
            <Button variant="ghost" size="icon" onClick={handleRefresh}>
              <RefreshCw className="h-5 w-5" />
            </Button>
            <Button variant="ghost" size="icon" onClick={handleLogout}>
              <LogOut className="h-5 w-5" />
            </Button>
          </div>
        </div>
      </header>

      {/* 主内容 */}
      <main className="container mx-auto px-4 md:px-8 py-6">
        {/* 统计卡片 */}
        <div className="grid gap-4 md:grid-cols-4 mb-6">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                凭据总数
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{data?.total || 0}</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                可调度凭据
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-green-600">
                {data?.dispatchable || 0}
              </div>
              <div className="text-xs text-muted-foreground mt-1">
                未禁用 {data?.available || 0}
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                当前活跃
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold flex items-center gap-2">
                #{data?.currentId || '-'}
                <Badge variant="success">活跃</Badge>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                当前排队
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {loadBalancingData?.waitingRequests ?? 0}
              </div>
            </CardContent>
          </Card>
        </div>

        <Card className="mb-6">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium">调度设置</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            <div className="space-y-2">
              <div className="text-sm text-muted-foreground">当前模式</div>
              <div className="flex items-center gap-2">
                <Badge variant="secondary">
                  {loadBalancingData?.mode === 'balanced' ? '均衡负载' : '优先级模式'}
                </Badge>
                <span className="text-sm text-muted-foreground">
                  {loadBalancingData?.waitingRequests ?? 0} 个请求排队中
                </span>
              </div>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium" htmlFor="queueMaxSize">
                最大排队数量
              </label>
              <Input
                id="queueMaxSize"
                type="number"
                min="0"
                step="1"
                value={queueMaxSizeInput}
                onChange={(e) => setQueueMaxSizeInput(e.target.value)}
                placeholder="0 表示关闭等待队列"
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium" htmlFor="queueMaxWaitMs">
                最大等待时间（毫秒）
              </label>
              <Input
                id="queueMaxWaitMs"
                type="number"
                min="0"
                step="100"
                value={queueMaxWaitMsInput}
                onChange={(e) => setQueueMaxWaitMsInput(e.target.value)}
                placeholder="0 表示关闭等待队列"
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium" htmlFor="rateLimitCooldownMs">
                429 冷却时间（毫秒）
              </label>
              <Input
                id="rateLimitCooldownMs"
                type="number"
                min="0"
                step="100"
                value={rateLimitCooldownMsInput}
                onChange={(e) => setRateLimitCooldownMsInput(e.target.value)}
                placeholder="0 表示关闭 429 冷却"
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium" htmlFor="defaultMaxConcurrency">
                默认单账号并发上限
              </label>
              <Input
                id="defaultMaxConcurrency"
                type="number"
                min="0"
                step="1"
                value={defaultMaxConcurrencyInput}
                onChange={(e) => setDefaultMaxConcurrencyInput(e.target.value)}
                placeholder="留空或 0 表示不限制"
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium" htmlFor="rateLimitBucketCapacity">
                Bucket 容量
              </label>
              <Input
                id="rateLimitBucketCapacity"
                type="number"
                min="0"
                step="0.1"
                value={rateLimitBucketCapacityInput}
                onChange={(e) => setRateLimitBucketCapacityInput(e.target.value)}
                placeholder="0 表示关闭 token bucket"
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium" htmlFor="rateLimitRefillPerSecond">
                基础回填速率（token/s）
              </label>
              <Input
                id="rateLimitRefillPerSecond"
                type="number"
                min="0"
                step="0.1"
                value={rateLimitRefillPerSecondInput}
                onChange={(e) => setRateLimitRefillPerSecondInput(e.target.value)}
                placeholder="0 表示关闭 token bucket"
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium" htmlFor="rateLimitRefillMinPerSecond">
                最小回填速率（token/s）
              </label>
              <Input
                id="rateLimitRefillMinPerSecond"
                type="number"
                min="0"
                step="0.1"
                value={rateLimitRefillMinPerSecondInput}
                onChange={(e) => setRateLimitRefillMinPerSecondInput(e.target.value)}
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium" htmlFor="rateLimitRefillRecoveryStep">
                成功恢复步长（token/s）
              </label>
              <Input
                id="rateLimitRefillRecoveryStep"
                type="number"
                min="0"
                step="0.1"
                value={rateLimitRefillRecoveryStepInput}
                onChange={(e) => setRateLimitRefillRecoveryStepInput(e.target.value)}
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium" htmlFor="rateLimitRefillBackoffFactor">
                429 衰减系数
              </label>
              <Input
                id="rateLimitRefillBackoffFactor"
                type="number"
                min="0.05"
                max="1"
                step="0.05"
                value={rateLimitRefillBackoffFactorInput}
                onChange={(e) => setRateLimitRefillBackoffFactorInput(e.target.value)}
              />
            </div>

            <Button
              onClick={handleSaveQueueSettings}
              disabled={isLoadingMode || isSettingMode}
              className="md:col-span-2 xl:col-span-3"
            >
              保存配置
            </Button>

            <p className="text-sm text-muted-foreground md:col-span-2 xl:col-span-3">
              `defaultMaxConcurrency` 是未单独设置账号的默认并发上限，`maxConcurrency` 控单账号覆盖值，token bucket 控单位时间发放速率，`rateLimitCooldownMs` 作为上游 `429` 后的固定短冷却兜底。建议先从 `queueMaxWaitMs=1000-3000`、`queueMaxSize=总并发的 1-2 倍`、`defaultMaxConcurrency=4-8`、`rateLimitCooldownMs=2000-5000`、`rateLimitBucketCapacity=2-4`、`rateLimitRefillPerSecond=0.5-1.5` 开始。
            </p>
          </CardContent>
        </Card>

        {/* 凭据列表 */}
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <h2 className="text-xl font-semibold">凭据管理</h2>
              {selectedIds.size > 0 && (
                <div className="flex items-center gap-2">
                  <Badge variant="secondary">已选择 {selectedIds.size} 个</Badge>
                  <Button onClick={deselectAll} size="sm" variant="ghost">
                    取消选择
                  </Button>
                </div>
              )}
            </div>
            <div className="flex gap-2">
              {selectedIds.size > 0 && (
                <>
                  <Button onClick={handleBatchVerify} size="sm" variant="outline">
                    <CheckCircle2 className="h-4 w-4 mr-2" />
                    批量验活
                  </Button>
                  <Button
                    onClick={handleBatchForceRefresh}
                    size="sm"
                    variant="outline"
                    disabled={batchRefreshing}
                  >
                    <RefreshCw className={`h-4 w-4 mr-2 ${batchRefreshing ? 'animate-spin' : ''}`} />
                    {batchRefreshing ? `刷新中... ${batchRefreshProgress.current}/${batchRefreshProgress.total}` : '批量刷新 Token'}
                  </Button>
                  <Button onClick={handleBatchResetFailure} size="sm" variant="outline">
                    <RotateCcw className="h-4 w-4 mr-2" />
                    恢复异常
                  </Button>
                  <Button
                    onClick={handleBatchDelete}
                    size="sm"
                    variant="destructive"
                    disabled={selectedDisabledCount === 0}
                    title={selectedDisabledCount === 0 ? '只能删除已禁用凭据' : undefined}
                  >
                    <Trash2 className="h-4 w-4 mr-2" />
                    批量删除
                  </Button>
                </>
              )}
              {verifying && !verifyDialogOpen && (
                <Button onClick={() => setVerifyDialogOpen(true)} size="sm" variant="secondary">
                  <CheckCircle2 className="h-4 w-4 mr-2 animate-spin" />
                  验活中... {verifyProgress.current}/{verifyProgress.total}
                </Button>
              )}
              {credentials.length > 0 && (
                <Button
                  onClick={handleQueryCurrentPageInfo}
                  size="sm"
                  variant="outline"
                  disabled={queryingInfo || currentCredentials.length === 0}
                  title={currentCredentials.length === 0 ? '当前筛选页没有可查询的账号' : undefined}
                >
                  <RefreshCw className={`h-4 w-4 mr-2 ${queryingInfo ? 'animate-spin' : ''}`} />
                  {queryingInfo ? `查询中... ${queryInfoProgress.current}/${queryInfoProgress.total}` : '查询信息'}
                </Button>
              )}
              {credentials.length > 0 && (
                <Button
                  onClick={handleClearAll}
                  size="sm"
                  variant="outline"
                  className="text-destructive hover:text-destructive"
                  disabled={disabledCredentialCount === 0}
                  title={disabledCredentialCount === 0 ? '没有可清除的已禁用凭据' : undefined}
                >
                  <Trash2 className="h-4 w-4 mr-2" />
                  清除已禁用
                </Button>
              )}
              <Button onClick={() => setKamImportDialogOpen(true)} size="sm" variant="outline">
                <FileUp className="h-4 w-4 mr-2" />
                Kiro Account Manager 导入
              </Button>
              <Button onClick={() => setBatchImportDialogOpen(true)} size="sm" variant="outline">
                <Upload className="h-4 w-4 mr-2" />
                批量导入
              </Button>
              <Button onClick={() => setAddDialogOpen(true)} size="sm">
                <Plus className="h-4 w-4 mr-2" />
                添加凭据
              </Button>
            </div>
          </div>
          <Card>
            <CardContent className="space-y-4 pt-6">
              <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_auto]">
                <div className="space-y-2">
                  <label className="text-sm font-medium" htmlFor="credential-search">
                    关键词搜索
                  </label>
                  <Input
                    id="credential-search"
                    value={searchKeyword}
                    onChange={(e) => setSearchKeyword(e.target.value)}
                    placeholder="搜索邮箱 / ID / 订阅等级 / 代理 / 禁用原因"
                  />
                </div>
                <div className="flex flex-wrap items-end gap-2">
                  <Button
                    type="button"
                    size="sm"
                    variant={quickFilter === 'dispatchable' ? 'default' : 'outline'}
                    onClick={() => setQuickFilter(prev => prev === 'dispatchable' ? 'all' : 'dispatchable')}
                  >
                    只看当前可调度
                  </Button>
                  <Button type="button" size="sm" variant="outline" onClick={resetFilters}>
                    重置筛选
                  </Button>
                </div>
              </div>

              <SegmentedTabs
                label="账号层级"
                options={levelOptions}
                value={levelFilter}
                onChange={setLevelFilter}
              />

              <SegmentedTabs
                label="启用状态"
                options={enabledOptions}
                value={enabledFilter}
                onChange={(value) => setEnabledFilter(value as EnabledFilter)}
              />

              <SegmentedTabs
                label="账号状态"
                options={accountStatusOptions}
                value={accountStatusFilter}
                onChange={(value) => setAccountStatusFilter(value as AccountStatusFilter)}
              />

              <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-end">
                <SegmentedTabs
                  label="排序方式"
                  options={sortOptions}
                  value={sortField}
                  onChange={handleSortFieldChange}
                />
                <div className="space-y-2">
                  <div className="text-sm font-medium">排序方向</div>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={() => setSortDirection(prev => prev === 'asc' ? 'desc' : 'asc')}
                  >
                    {sortDirection === 'asc' ? '升序' : '降序'}
                  </Button>
                </div>
              </div>

              <div className="flex flex-wrap items-center justify-between gap-2 border-t pt-4 text-sm text-muted-foreground">
                <span>
                  筛选后 {filteredCredentials.length} / {credentials.length} 个账号
                </span>
                <div className="flex flex-wrap gap-2">
                  <Badge variant="secondary">补充筛选：关键词搜索</Badge>
                  <Badge variant="secondary">补充筛选：只看当前可调度</Badge>
                  <Badge variant="secondary">补充排序：最后调用</Badge>
                </div>
              </div>
            </CardContent>
          </Card>

          {credentials.length === 0 ? (
            <Card>
              <CardContent className="py-8 text-center text-muted-foreground">
                暂无凭据
              </CardContent>
            </Card>
          ) : filteredCredentials.length === 0 ? (
            <Card>
              <CardContent className="py-8 text-center text-muted-foreground space-y-3">
                <div>当前筛选条件下没有匹配的账号</div>
                <Button type="button" variant="outline" size="sm" onClick={resetFilters}>
                  清空筛选条件
                </Button>
              </CardContent>
            </Card>
          ) : (
            <>
              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                {currentCredentials.map((credential) => (
                  <CredentialCard
                    key={credential.id}
                    credential={credential}
                    onViewBalance={handleViewBalance}
                    selected={selectedIds.has(credential.id)}
                    onToggleSelect={() => toggleSelect(credential.id)}
                    balance={balanceMap.get(credential.id) || null}
                    loadingBalance={loadingBalanceIds.has(credential.id)}
                  />
                ))}
              </div>

              {/* 分页控件 */}
              {totalPages > 1 && (
                <div className="flex justify-center items-center gap-4 mt-6">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                    disabled={effectiveCurrentPage === 1}
                  >
                    上一页
                  </Button>
                  <span className="text-sm text-muted-foreground">
                    第 {effectiveCurrentPage} / {totalPages} 页（共 {filteredCredentials.length} 个账号）
                  </span>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                    disabled={effectiveCurrentPage === totalPages}
                  >
                    下一页
                  </Button>
                </div>
              )}
            </>
          )}
        </div>
      </main>

      {/* 余额对话框 */}
      <BalanceDialog
        credentialId={selectedCredentialId}
        open={balanceDialogOpen}
        onOpenChange={setBalanceDialogOpen}
      />

      {/* 添加凭据对话框 */}
      <AddCredentialDialog
        open={addDialogOpen}
        onOpenChange={setAddDialogOpen}
      />

      {/* 批量导入对话框 */}
      <BatchImportDialog
        open={batchImportDialogOpen}
        onOpenChange={setBatchImportDialogOpen}
      />

      {/* KAM 账号导入对话框 */}
      <KamImportDialog
        open={kamImportDialogOpen}
        onOpenChange={setKamImportDialogOpen}
      />

      {/* 批量验活对话框 */}
      <BatchVerifyDialog
        open={verifyDialogOpen}
        onOpenChange={setVerifyDialogOpen}
        verifying={verifying}
        progress={verifyProgress}
        results={verifyResults}
        onCancel={handleCancelVerify}
      />
    </div>
  )
}
