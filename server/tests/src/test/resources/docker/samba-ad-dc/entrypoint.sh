#!/bin/bash
set -e

export _REALM=${_REALM:-INFINISPAN.ORG}
export _DOMAIN=${_DOMAIN:-${_REALM%%.*}}
export _NETBIOS=${_NETBIOS:-dc01}
export _PASSWORD=${_PASSWORD:-'strongPassword@123'}
export _DNS_FORWARDER=${_DNS_FORWARDER:-1.1.1.1 8.8.8.8}

if [ ! -f /etc/samba/smb.conf ]; then
   echo "Provisioning Samba $(samba --version) domain ${_DOMAIN} realm ${_REALM}"
   samba-tool domain provision \
      --server-role=dc \
      --realm="${_REALM}" \
      --domain="${_DOMAIN}" \
      --use-rfc2307 \
      --adminpass="${_PASSWORD}" \
      --dns-backend=SAMBA_INTERNAL \
      --option="dns forwarder = ${_DNS_FORWARDER}" \
      --option="template shell = /bin/bash"
   samba-tool user setexpiry Administrator --noexpiry
   # Relax the password policy and disable Samba's own account lockout so that
   # Infinispan's brute-force protection is the only thing under test.
   samba-tool domain passwordsettings set \
      --complexity=off --min-pwd-length=1 --max-pwd-age=365 --min-pwd-age=0 \
      --account-lockout-threshold=0 --reset-account-lockout-after=0 --account-lockout-duration=0
   # Samba's LDAP server requires strong auth for simple binds by default. The
   # provision --option for this parameter is silently dropped, so set it in
   # smb.conf directly. The Infinispan LDAP test config talks over plain ldap://
   # (matching the Apache Directory server setup), which needs this relaxed.
   {
      echo ""
      echo "[global]"
      echo "ldap server require strong auth = no"
   } >> /etc/samba/smb.conf
 fi

cp /var/lib/samba/private/krb5.conf /etc/ 2>/dev/null || true

exec /usr/sbin/samba -i -M single
