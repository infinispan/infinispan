package org.infinispan.query.core.stats.impl;

import java.util.Collections;
import java.util.Map;

import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.core.stats.IndexStatistics;

/**
 * Empty index statistics for non-indexed Caches.
 *
 * @since 12.0
 */
public class EmptyIndexStatistics implements IndexStatistics {

   public static final IndexStatistics INSTANCE = new EmptyIndexStatistics();

   private EmptyIndexStatistics() {
   }

   @Override
   public Map<String, IndexInfo> indexInfos() {
      return Collections.emptyMap();
   }

   public IndexStatistics getSnapshot() {
      return this;
   }

   @Override
   public IndexStatistics merge(IndexStatistics other) {
      return other;
   }

}
