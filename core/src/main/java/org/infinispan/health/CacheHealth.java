package org.infinispan.health;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 * Cache health information.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public interface CacheHealth extends JsonSerialization {

   /**
    * Returns Cache name.
    */
   String getCacheName();

   /**
    * Returns Cache health status.
    */
   HealthStatus getStatus();

   default Json toJson() {
      return Json.object().set("status", getStatus()).set("cache_name", getCacheName());
   }
}
