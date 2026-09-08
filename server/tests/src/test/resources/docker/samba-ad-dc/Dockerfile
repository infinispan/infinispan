FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        samba samba-ad-provision samba-dsdb-modules samba-vfs-modules samba-common-bin \
        smbclient krb5-config ldb-tools winbind ca-certificates ldap-utils \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /etc/samba/smb.conf /var/run/samba/* /var/lib/samba/* /var/log/samba/*log* /etc/krb5.conf

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 53/udp 53/tcp 88/tcp 88/udp 389/tcp 636/tcp

ENTRYPOINT ["/entrypoint.sh"]
