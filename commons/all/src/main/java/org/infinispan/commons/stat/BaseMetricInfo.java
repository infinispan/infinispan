package org.infinispan.commons.stat;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Base implementation of {@link MetricInfo} with all the required information.
 *
 * @since 15.0
 */
abstract class BaseMetricInfo implements MetricInfo {

   private final String name;
   private final String description;
   private final Map<String, String> map;

   BaseMetricInfo(String name, String description, Map<String, String> map) {
      this.name = Objects.requireNonNull(name);
      this.description = Objects.requireNonNull(description);
      this.map = map == null || map.isEmpty() ? Collections.emptyMap() : Map.copyOf(map);
   }

   @Override
   public final String getName() {
      return name;
   }

   @Override
   public final String getDescription() {
      return description;
   }

   @Override
   public final Map<String, String> getTags() {
      return map;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "name='" + getName() + '\'' +
            ", description='" + getDescription() + '\'' +
            ", tags=" + getTags() +
            '}';
   }
}
