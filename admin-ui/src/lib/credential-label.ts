import type { CredentialStatusItem } from '@/types/api'

type CredentialIdentity = Pick<CredentialStatusItem, 'id' | 'email' | 'userId'>

export function getCredentialLabel(credential: CredentialIdentity | null | undefined): string {
  const email = credential?.email?.trim()
  if (email) {
    return email
  }
  const userId = credential?.userId?.trim()
  if (userId) {
    return userId
  }

  return credential ? `凭据 #${credential.id}` : '凭据'
}

export function getCredentialLabelWithId(
  credential: CredentialIdentity | null | undefined
): string {
  if (!credential) {
    return '凭据'
  }

  const email = credential?.email?.trim()
  if (email) {
    return `${email}（#${credential.id}）`
  }
  const userId = credential?.userId?.trim()
  if (userId) {
    return `${userId}（#${credential.id}）`
  }

  return getCredentialLabel(credential)
}
