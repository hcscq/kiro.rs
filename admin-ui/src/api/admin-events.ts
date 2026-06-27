import type { AdminStateEvent } from '@/types/api'

interface SubscribeAdminEventsOptions {
  apiKey: string
  signal: AbortSignal
  onState: (event: AdminStateEvent) => void
}

interface ParsedSseEvent {
  event: string
  data: string
}

function parseSseFrame(frame: string): ParsedSseEvent | null {
  let event = 'message'
  const data: string[] = []

  for (const line of frame.split('\n')) {
    if (!line || line.startsWith(':')) {
      continue
    }

    const separatorIndex = line.indexOf(':')
    const field = separatorIndex === -1 ? line : line.slice(0, separatorIndex)
    const rawValue = separatorIndex === -1 ? '' : line.slice(separatorIndex + 1)
    const value = rawValue.startsWith(' ') ? rawValue.slice(1) : rawValue

    if (field === 'event') {
      event = value
    } else if (field === 'data') {
      data.push(value)
    }
  }

  if (data.length === 0) {
    return null
  }

  return { event, data: data.join('\n') }
}

export async function subscribeAdminEvents({
  apiKey,
  signal,
  onState,
}: SubscribeAdminEventsOptions): Promise<void> {
  const response = await fetch('/api/admin/events', {
    method: 'GET',
    headers: {
      Accept: 'text/event-stream',
      'x-api-key': apiKey,
    },
    cache: 'no-store',
    signal,
  })

  if (!response.ok) {
    throw new Error(`Admin events failed with HTTP ${response.status}`)
  }

  if (!response.body) {
    throw new Error('Admin events response body is not readable')
  }

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  while (!signal.aborted) {
    const { value, done } = await reader.read()
    if (done) {
      break
    }

    buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n')

    let frameEnd = buffer.indexOf('\n\n')
    while (frameEnd !== -1) {
      const frame = buffer.slice(0, frameEnd)
      buffer = buffer.slice(frameEnd + 2)

      const event = parseSseFrame(frame)
      if (event?.event === 'state') {
        try {
          onState(JSON.parse(event.data) as AdminStateEvent)
        } catch (error) {
          console.warn('Failed to parse admin state event', error)
        }
      }

      frameEnd = buffer.indexOf('\n\n')
    }
  }
}
