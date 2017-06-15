package org.infinispan.query.remote.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.IdentityEncoder;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
final class CompatibilityQueryEngine extends BaseRemoteQueryEngine {

   CompatibilityQueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed) {
      super(cache.getAdvancedCache().withEncoding(IdentityEncoder.class), isIndexed, CompatibilityReflectionMatcher.class, null);
   }
}
