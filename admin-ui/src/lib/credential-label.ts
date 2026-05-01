import type { CredentialStatusItem } from '@/types/api'

type CredentialIdentity = Pick<CredentialStatusItem, 'id' | 'email'>

export function getCredentialLabel(credential: CredentialIdentity | null | undefined): string {
  const email = credential?.email?.trim()
  if (email) {
    return email
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

  return getCredentialLabel(credential)
}
