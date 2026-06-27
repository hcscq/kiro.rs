import { useEffect, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { subscribeAdminEvents } from '@/api/admin-events'
import { storage } from '@/lib/storage'
import type { AdminStateEvent } from '@/types/api'

const RECONNECT_BASE_DELAY_MS = 1000
const RECONNECT_MAX_DELAY_MS = 30000
const HOT_STATE_CREDENTIAL_REFRESH_MS = 5000

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason)
      return
    }

    const timeout = window.setTimeout(resolve, ms)
    signal.addEventListener(
      'abort',
      () => {
        window.clearTimeout(timeout)
        reject(signal.reason)
      },
      { once: true }
    )
  })
}

function credentialsChanged(previous: AdminStateEvent, next: AdminStateEvent): boolean {
  return (
    previous.credentialsRevision !== next.credentialsRevision ||
    previous.credentialsFingerprint !== next.credentialsFingerprint ||
    previous.balanceCacheRevision !== next.balanceCacheRevision
  )
}

function dispatchChanged(previous: AdminStateEvent, next: AdminStateEvent): boolean {
  return (
    previous.dispatchRevision !== next.dispatchRevision ||
    previous.dispatchFingerprint !== next.dispatchFingerprint ||
    previous.waitingRequests !== next.waitingRequests
  )
}

export function AdminEventsBridge() {
  const queryClient = useQueryClient()
  const previousEventRef = useRef<AdminStateEvent | null>(null)
  const lastHotCredentialsRefreshRef = useRef(0)

  useEffect(() => {
    const apiKey = storage.getApiKey()
    if (!apiKey) {
      return
    }

    const controller = new AbortController()

    const handleState = (event: AdminStateEvent) => {
      const previous = previousEventRef.current
      previousEventRef.current = event

      if (!previous) {
        return
      }

      if (credentialsChanged(previous, event)) {
        lastHotCredentialsRefreshRef.current = Date.now()
        queryClient.invalidateQueries({ queryKey: ['credentials'] })
      } else if (
        previous.inFlight !== event.inFlight ||
        previous.dispatchable !== event.dispatchable ||
        previous.rateLimited !== event.rateLimited ||
        previous.abnormal !== event.abnormal
      ) {
        const now = Date.now()
        if (now - lastHotCredentialsRefreshRef.current >= HOT_STATE_CREDENTIAL_REFRESH_MS) {
          lastHotCredentialsRefreshRef.current = now
          queryClient.invalidateQueries({ queryKey: ['credentials'] })
        }
      }

      if (dispatchChanged(previous, event)) {
        queryClient.invalidateQueries({ queryKey: ['loadBalancingMode'] })
      }
    }

    const run = async () => {
      let attempt = 0

      while (!controller.signal.aborted) {
        try {
          await subscribeAdminEvents({
            apiKey,
            signal: controller.signal,
            onState: handleState,
          })
          attempt = 0
        } catch (error) {
          if (controller.signal.aborted) {
            break
          }
          console.warn('Admin events connection failed', error)
        }

        const delay = Math.min(
          RECONNECT_BASE_DELAY_MS * 2 ** attempt,
          RECONNECT_MAX_DELAY_MS
        )
        attempt += 1
        try {
          await sleep(delay, controller.signal)
        } catch {
          break
        }
      }
    }

    void run()

    return () => {
      controller.abort()
    }
  }, [queryClient])

  return null
}
