#!/bin/bash
export hostName=$(cat /etc/hosts|tail -1|awk '{print $1}')
/opt/eap/bin/kcadm.sh config credentials --server http://$hostName:8080/auth --realm master --user keycloak --password keycloak;
/opt/eap/bin/kcadm.sh create realms -s realm=infinispan -s enabled=true -o -f /tmp/infinispanTempFiles/keycloak/keycloak.json;
