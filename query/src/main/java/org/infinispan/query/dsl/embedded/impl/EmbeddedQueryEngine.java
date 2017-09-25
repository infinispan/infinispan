package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.objectfilter.impl.ReflectionMatcher;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class EmbeddedQueryEngine extends QueryEngine<Class<?>> {

   public EmbeddedQueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed) {
      super(cache, isIndexed, ReflectionMatcher.class, null);
   }
}
