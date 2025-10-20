#!/bin/bash -x

ROOTFS="$1"
WORKDIR="$2"

unzip /tmp/infinispan -d /tmp
mv /tmp/infinispan-* "${ROOTFS}${WORKDIR}"
chmod -R g+rwX "${ROOTFS}${WORKDIR}"
