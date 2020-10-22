package org.infinispan.query.core.stats;

import java.util.Map;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 *
 * Exposes index statistics for a particular cache.
 *
 * @since 12.0
 */
public interface IndexStatistics extends JsonSerialization {

   /**
    * @return The {@link IndexInfo} for each indexed entity configured in the cache. The name of the entity is
    * either the class name annotated with @Index, or the protobuf Message name.
    */
   Map<String, IndexInfo> indexInfos();

   /**
    * Merge with another {@link IndexStatistics}.
    *
    * @return self
    */
   IndexStatistics merge(IndexStatistics other);

   IndexStatistics getSnapshot();

   default boolean reindexing() {
      return false;
   }

   @Override
   default Json toJson() {
      return Json.object()
            .set("types", Json.make(indexInfos()))
            .set("reindexing", reindexing());
   }


}
