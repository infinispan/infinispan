package org.infinispan.query.remote.impl;

import org.infinispan.AdvancedCache;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
final class CompatibilityQueryEngine extends BaseRemoteQueryEngine {

   CompatibilityQueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed) {
      super(cache, isIndexed, CompatibilityReflectionMatcher.class, null);
   }
}
