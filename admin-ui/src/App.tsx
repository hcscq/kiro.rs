import { useState, useEffect } from 'react'
import { storage } from '@/lib/storage'
import { LoginPage } from '@/components/login-page'
import { Dashboard } from '@/components/dashboard'
import { SettingsPage } from '@/components/settings-page'
import { MainLayout } from '@/components/layout/main-layout'
import { Toaster } from '@/components/ui/sonner'
import { useQueryClient } from '@tanstack/react-query'

function App() {
  const [isLoggedIn, setIsLoggedIn] = useState(false)
  const [activeTab, setActiveTab] = useState('dashboard')
  const queryClient = useQueryClient()

  useEffect(() => {
    // 检查是否已经有保存的 API Key
    if (storage.getApiKey()) {
      setIsLoggedIn(true)
    }
  }, [])

  const handleLogin = () => {
    setActiveTab('dashboard')
    setIsLoggedIn(true)
  }

  const handleLogout = () => {
    storage.removeApiKey()
    queryClient.clear()
    setActiveTab('dashboard')
    setIsLoggedIn(false)
  }

  return (
    <>
      {isLoggedIn ? (
        <MainLayout 
          activeTab={activeTab} 
          onTabChange={setActiveTab} 
          onLogout={handleLogout}
        >
          {activeTab === 'dashboard' ? <Dashboard /> : <SettingsPage />}
        </MainLayout>
      ) : (
        <LoginPage onLogin={handleLogin} />
      )}
      <Toaster position="top-right" />
    </>
  )
}

export default App
