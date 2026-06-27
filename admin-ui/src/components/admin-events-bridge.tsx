import { useEffect, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { subscribeAdminEvents } from '@/api/admin-events'
import { getCredentialsDelta } from '@/api/credentials'
import { storage } from '@/lib/storage'
import type {
  AdminStateEvent,
  CredentialsDeltaResponse,
  CredentialsStatusResponse,
} from '@/types/api'

const RECONNECT_BASE_DELAY_MS = 1000
const RECONNECT_MAX_DELAY_MS = 30000

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

function mergeCredentialsDelta(
  current: CredentialsStatusResponse,
  delta: CredentialsDeltaResponse
): CredentialsStatusResponse {
  const deletedIds = new Set(delta.deletedIds)
  const upserts = new Map(delta.upserts.map((credential) => [credential.id, credential]))
  const credentials = current.credentials
    .filter((credential) => !deletedIds.has(credential.id))
    .map((credential) => upserts.get(credential.id) ?? credential)

  for (const credential of delta.upserts) {
    if (!current.credentials.some((item) => item.id === credential.id)) {
      credentials.push(credential)
    }
  }

  credentials.sort((left, right) => left.priority - right.priority || left.id - right.id)

  return {
    ...current,
    total: delta.total,
    available: delta.available,
    dispatchable: delta.dispatchable,
    currentId: delta.currentId,
    credentialsRevision: delta.revision,
    balanceCacheRevision: delta.balanceRevision,
    credentialsFingerprint: delta.fingerprint,
    credentials,
  }
}

function applyStateSummary(
  current: CredentialsStatusResponse,
  event: AdminStateEvent
): CredentialsStatusResponse {
  if (
    current.total === event.total &&
    current.available === event.available &&
    current.dispatchable === event.dispatchable &&
    current.currentId === event.currentId
  ) {
    return current
  }

  return {
    ...current,
    total: event.total,
    available: event.available,
    dispatchable: event.dispatchable,
    currentId: event.currentId,
    credentials: current.credentials.map((credential) => ({
      ...credential,
      isCurrent: credential.id === event.currentId,
    })),
  }
}

export function AdminEventsBridge() {
  const queryClient = useQueryClient()
  const previousEventRef = useRef<AdminStateEvent | null>(null)
  const deltaInFlightRef = useRef(false)
  const deltaPendingRef = useRef(false)

  useEffect(() => {
    const apiKey = storage.getApiKey()
    if (!apiKey) {
      return
    }

    const controller = new AbortController()

    const refreshCredentialsDelta = async () => {
      if (deltaInFlightRef.current) {
        deltaPendingRef.current = true
        return
      }

      deltaInFlightRef.current = true

      try {
        do {
          deltaPendingRef.current = false
          const current = queryClient.getQueryData<CredentialsStatusResponse>(['credentials'])

          if (!current) {
            await queryClient.invalidateQueries({ queryKey: ['credentials'] })
            continue
          }

          const delta = await getCredentialsDelta({
            sinceRevision: current.credentialsRevision ?? 0,
            balanceCacheRevision: current.balanceCacheRevision ?? 0,
            credentialsFingerprint: current.credentialsFingerprint ?? 0,
            knownCredentials: current.credentials.map((credential) => ({
              id: credential.id,
              fingerprint: credential.fingerprint ?? 0,
            })),
          })

          if (delta.resetRequired) {
            await queryClient.invalidateQueries({ queryKey: ['credentials'] })
            continue
          }

          queryClient.setQueryData<CredentialsStatusResponse>(['credentials'], (latest) => {
            if (!latest) {
              return latest
            }
            return mergeCredentialsDelta(latest, delta)
          })
        } while (deltaPendingRef.current && !controller.signal.aborted)
      } catch (error) {
        if (!controller.signal.aborted) {
          console.warn('Failed to refresh admin credentials delta', error)
          await queryClient.invalidateQueries({ queryKey: ['credentials'] })
        }
      } finally {
        deltaInFlightRef.current = false
      }
    }

    const handleState = (event: AdminStateEvent) => {
      const previous = previousEventRef.current
      previousEventRef.current = event

      if (!previous) {
        return
      }

      queryClient.setQueryData<CredentialsStatusResponse>(['credentials'], (current) =>
        current ? applyStateSummary(current, event) : current
      )

      if (credentialsChanged(previous, event)) {
        void refreshCredentialsDelta()
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
