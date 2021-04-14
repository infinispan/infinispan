package org.infinispan.server.configuration.security;

import java.util.List;

/**
 * @since 10.0
 */
public class RealmsConfiguration {
   private final List<RealmConfiguration> realms;

   RealmsConfiguration(List<RealmConfiguration> realms) {
      this.realms = realms;
   }

   public List<RealmConfiguration> realms() {
      return realms;
   }
}
