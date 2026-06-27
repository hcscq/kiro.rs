import { useEffect, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { subscribeAdminEvents } from '@/api/admin-events'
import { getCredentialsDelta, getCredentialsRuntimeDelta } from '@/api/credentials'
import { storage } from '@/lib/storage'
import type {
  AdminStateEvent,
  CredentialsDeltaResponse,
  CredentialsRuntimeDeltaResponse,
  CredentialsStatusResponse,
} from '@/types/api'

const RECONNECT_BASE_DELAY_MS = 1000
const RECONNECT_MAX_DELAY_MS = 30000
const RUNTIME_DELTA_MIN_INTERVAL_MS = 5000

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

function runtimeChanged(previous: AdminStateEvent, next: AdminStateEvent): boolean {
  return (
    previous.inFlight !== next.inFlight ||
    previous.dispatchable !== next.dispatchable ||
    previous.rateLimited !== next.rateLimited ||
    previous.abnormal !== next.abnormal
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

function mergeCredentialsRuntimeDelta(
  current: CredentialsStatusResponse,
  delta: CredentialsRuntimeDeltaResponse
): CredentialsStatusResponse {
  if (delta.updates.length === 0 && delta.deletedIds.length === 0) {
    return current
  }

  const deletedIds = new Set(delta.deletedIds)
  const updates = new Map(delta.updates.map((runtime) => [runtime.id, runtime]))

  return {
    ...current,
    credentials: current.credentials
      .filter((credential) => !deletedIds.has(credential.id))
      .map((credential) => {
        const runtime = updates.get(credential.id)
        if (!runtime) {
          return credential
        }

        return {
          ...credential,
          runtimeFingerprint: runtime.runtimeFingerprint,
          successCount: runtime.successCount,
          tokenUsageCount: runtime.tokenUsageCount,
          inputTokens: runtime.inputTokens,
          outputTokens: runtime.outputTokens,
          totalTokens: runtime.totalTokens,
          lastUsedAt: runtime.lastUsedAt,
          inFlight: runtime.inFlight,
          cooldownRemainingMs: runtime.cooldownRemainingMs,
          rateLimitBucketTokens: runtime.rateLimitBucketTokens,
          rateLimitBucketCapacity: runtime.rateLimitBucketCapacity,
          rateLimitRefillPerSecond: runtime.rateLimitRefillPerSecond,
          rateLimitHitStreak: runtime.rateLimitHitStreak,
          nextReadyInMs: runtime.nextReadyInMs,
        }
      }),
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
  const runtimeDeltaInFlightRef = useRef(false)
  const runtimeDeltaPendingRef = useRef(false)
  const lastRuntimeDeltaAtRef = useRef(0)
  const runtimeDeltaTimerRef = useRef<number | null>(null)

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

    const refreshCredentialsRuntimeDelta = async () => {
      const now = Date.now()
      if (now - lastRuntimeDeltaAtRef.current < RUNTIME_DELTA_MIN_INTERVAL_MS) {
        runtimeDeltaPendingRef.current = true
        if (runtimeDeltaTimerRef.current === null) {
          runtimeDeltaTimerRef.current = window.setTimeout(() => {
            runtimeDeltaTimerRef.current = null
            void refreshCredentialsRuntimeDelta()
          }, RUNTIME_DELTA_MIN_INTERVAL_MS - (now - lastRuntimeDeltaAtRef.current))
        }
        return
      }

      if (runtimeDeltaInFlightRef.current) {
        runtimeDeltaPendingRef.current = true
        return
      }

      runtimeDeltaInFlightRef.current = true

      try {
        do {
          runtimeDeltaPendingRef.current = false
          lastRuntimeDeltaAtRef.current = Date.now()
          const current = queryClient.getQueryData<CredentialsStatusResponse>(['credentials'])

          if (!current) {
            return
          }

          const delta = await getCredentialsRuntimeDelta({
            knownRuntime: current.credentials.map((credential) => ({
              id: credential.id,
              runtimeFingerprint: credential.runtimeFingerprint ?? 0,
            })),
          })

          queryClient.setQueryData<CredentialsStatusResponse>(['credentials'], (latest) => {
            if (!latest) {
              return latest
            }
            return mergeCredentialsRuntimeDelta(latest, delta)
          })

          if (runtimeDeltaPendingRef.current && !controller.signal.aborted) {
            const elapsed = Date.now() - lastRuntimeDeltaAtRef.current
            if (elapsed < RUNTIME_DELTA_MIN_INTERVAL_MS) {
              await sleep(RUNTIME_DELTA_MIN_INTERVAL_MS - elapsed, controller.signal)
            }
          }
        } while (runtimeDeltaPendingRef.current && !controller.signal.aborted)
      } catch (error) {
        if (!controller.signal.aborted) {
          console.warn('Failed to refresh admin credentials runtime delta', error)
        }
      } finally {
        runtimeDeltaInFlightRef.current = false
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

      if (runtimeChanged(previous, event)) {
        void refreshCredentialsRuntimeDelta()
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
      if (runtimeDeltaTimerRef.current !== null) {
        window.clearTimeout(runtimeDeltaTimerRef.current)
      }
      controller.abort()
    }
  }, [queryClient])

  return null
}
