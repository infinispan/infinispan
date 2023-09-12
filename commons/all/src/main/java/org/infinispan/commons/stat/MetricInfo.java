package org.infinispan.commons.stat;

import java.util.Collections;
import java.util.Map;

/**
 * Class that represent the information about a metrics.
 * <p>
 * Includes the metrics's name, description and tags. Subclasses can add more information about it.
 *
 * @since 15.0
 */
public interface MetricInfo {

   /**
    * @return The metrics name.
    */
   String getName();

   /**
    * @return The metrics description/help message.
    */
   String getDescription();

   /**
    * @return The tags to be used. Must be non-null.
    */
   default Map<String, String> getTags() {
      return Collections.emptyMap();
   }
}
