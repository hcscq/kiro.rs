import { ReactNode, useState } from 'react'
import { Server, Moon, Sun, LogOut, RefreshCw, LayoutDashboard, Settings } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { toast } from 'sonner'
import { useLoadBalancingMode, useSetLoadBalancingMode } from '@/hooks/use-credentials'
import { extractErrorMessage } from '@/lib/utils'
import { useQueryClient } from '@tanstack/react-query'

interface MainLayoutProps {
  children: ReactNode
  activeTab: string
  onTabChange: (tab: string) => void
  onLogout: () => void
}

export function MainLayout({ children, activeTab, onTabChange, onLogout }: MainLayoutProps) {
  const [darkMode, setDarkMode] = useState(() => {
    if (typeof window !== 'undefined') {
      return document.documentElement.classList.contains('dark')
    }
    return false
  })

  const queryClient = useQueryClient()
  const { data: loadBalancingData, isLoading: isLoadingMode } = useLoadBalancingMode()
  const { mutate: setLoadBalancingMode, isPending: isSettingMode } = useSetLoadBalancingMode()

  const toggleDarkMode = () => {
    setDarkMode(!darkMode)
    document.documentElement.classList.toggle('dark')
  }

  const handleGlobalRefresh = () => {
    // 强制验证所有查询使之重新拉取数据
    queryClient.invalidateQueries()
    toast.success('已全局刷新数据')
  }

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

  return (
    <div className="flex h-screen w-full overflow-hidden bg-background">
      {/* Sidebar */}
      <aside className="w-56 border-r bg-muted/10 flex flex-col hidden md:flex">
        <div className="h-14 flex flex-shrink-0 items-center justify-center border-b font-semibold gap-2">
          <Server className="h-5 w-5 text-primary" />
          <span className="text-lg">Kiro Admin</span>
        </div>
        
        <div className="flex flex-col gap-2 p-4 flex-1">
          <Button 
            variant={activeTab === 'dashboard' ? 'secondary' : 'ghost'} 
            className={cn("justify-start font-medium", activeTab === 'dashboard' && 'bg-secondary')}
            onClick={() => onTabChange('dashboard')}
          >
            <LayoutDashboard className="h-4 w-4 mr-3" />
            凭据管理
          </Button>
          <Button 
            variant={activeTab === 'settings' ? 'secondary' : 'ghost'} 
            className={cn("justify-start font-medium", activeTab === 'settings' && 'bg-secondary')}
            onClick={() => onTabChange('settings')}
          >
            <Settings className="h-4 w-4 mr-3" />
            调度设置
          </Button>
        </div>
      </aside>

      {/* Main Container */}
      <div className="flex flex-col flex-1 overflow-hidden">
        {/* Header */}
        <header className="sticky top-0 z-10 flex-shrink-0 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
          <div className="flex h-14 items-center justify-between px-4 md:px-8">
            <div className="flex items-center gap-2 md:hidden">
              <Server className="h-5 w-5" />
              <span className="font-semibold">Kiro Admin</span>
            </div>
            
            <div className="font-semibold text-sm text-muted-foreground hidden md:block">
              {activeTab === 'dashboard' ? 'Kiro / 凭据管理' : 'Kiro / 调度设置'}
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
              <Button variant="ghost" size="icon" onClick={handleGlobalRefresh}>
                <RefreshCw className="h-5 w-5" />
              </Button>
              <Button variant="ghost" size="icon" onClick={onLogout}>
                <LogOut className="h-5 w-5" />
              </Button>
            </div>
          </div>
        </header>

        <div className="border-b bg-background px-4 py-2 md:hidden">
          <div className="flex gap-2">
            <Button
              variant={activeTab === 'dashboard' ? 'secondary' : 'ghost'}
              className={cn(
                'flex-1 justify-center',
                activeTab === 'dashboard' && 'bg-secondary'
              )}
              onClick={() => onTabChange('dashboard')}
            >
              <LayoutDashboard className="h-4 w-4 mr-2" />
              凭据管理
            </Button>
            <Button
              variant={activeTab === 'settings' ? 'secondary' : 'ghost'}
              className={cn(
                'flex-1 justify-center',
                activeTab === 'settings' && 'bg-secondary'
              )}
              onClick={() => onTabChange('settings')}
            >
              <Settings className="h-4 w-4 mr-2" />
              调度设置
            </Button>
          </div>
        </div>
        
        {/* Main Content Area */}
        <main className="flex-1 overflow-y-auto w-full">
          <div className="p-4 md:p-6">
            {children}
          </div>
        </main>
      </div>
    </div>
  )
}
