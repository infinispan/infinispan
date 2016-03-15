package org.infinispan.query.continuous;

import org.infinispan.Cache;
import org.infinispan.query.continuous.impl.ContinuousQueryImpl;

/**
 * A container of continuous query listeners for a cache.
 * <p>This class is not threadsafe.
 *
 * @author anistor@redhat.com
 * @since 8.0
 * @deprecated replaced by {@link org.infinispan.query.api.continuous.ContinuousQuery}; will be removed in 8.3
 */
@Deprecated
public final class ContinuousQuery<K, V> extends ContinuousQueryImpl<K, V> {

   public ContinuousQuery(Cache<K, V> cache) {
      super(cache);
   }
}
