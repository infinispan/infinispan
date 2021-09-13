package org.infinispan.server.configuration.security;

import java.util.Map;
import java.util.Properties;

import org.infinispan.server.Server;

/**
 * @since 10.0
 */
public class RealmsConfiguration {
   private final Map<String, RealmConfiguration> realms;

   RealmsConfiguration(Map<String, RealmConfiguration> realms) {
      this.realms = realms;
   }

   public Map<String, RealmConfiguration> realms() {
      return realms;
   }

   public RealmConfiguration getRealm(String name) {
      RealmConfiguration realm = realms.get(name);
      if (realm == null) {
         throw Server.log.unknownSecurityDomain(name);
      } else {
         return realm;
      }
   }

   void init(SecurityConfiguration security, Properties properties) {
      for(RealmConfiguration realm : realms.values()) {
         realm.init(security, properties);
      }
   }
}
