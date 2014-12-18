package org.infinispan.client.hotrod.configuration;

public enum NearCacheMode {

   DISABLED, LAZY, EAGER;

   public boolean enabled() {
      return this != DISABLED;
   }

   public boolean eager() {
      return this == EAGER;
   }

}
