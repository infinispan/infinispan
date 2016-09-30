package org.infinispan.server.endpoint.subsystem;

import java.util.Map;

import org.jboss.as.domain.management.SecurityRealm;
import org.jboss.msc.value.InjectedValue;

public interface EncryptableService {
   InjectedValue<SecurityRealm> getEncryptionSecurityRealm();

   InjectedValue<SecurityRealm> getSniSecurityRealm(String sniHostName);

   Map<String, InjectedValue<SecurityRealm>> getSniConfiguration();

   String getServerName();

   boolean getClientAuth();

   void setClientAuth(boolean enabled);
}
