package org.infinispan.health.impl;

import org.infinispan.health.CacheHealth;
import org.infinispan.health.HealthStatus;

/**
 * @since 12.0
 */
final class InvalidCacheHealth implements CacheHealth {
   private final String name;

   public InvalidCacheHealth(String name) {
      this.name = name;
   }

   @Override
   public String getCacheName() {
      return name;
   }

   @Override
   public HealthStatus getStatus() {
      return HealthStatus.FAILED;
   }
}
