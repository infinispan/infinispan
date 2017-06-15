package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.objectfilter.impl.ReflectionMatcher;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class EmbeddedQueryEngine extends QueryEngine<Class<?>> {

   public EmbeddedQueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed) {
      super(cache.withEncoding(IdentityEncoder.class), isIndexed, ReflectionMatcher.class, null);
   }
}
