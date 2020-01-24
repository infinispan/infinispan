package org.infinispan.stats;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Similar to {@link Stats} but in the scope of a single per node CacheContainer.
 *
 * @author Vladimir Blagojevic
 * @since 7.1
 * @deprecated Since 10.1.3. This mixes statistics across unrelated caches so the reported numbers don't have too much
 * relevance. Please use {@link org.infinispan.stats.Stats} or {@link org.infinispan.stats.ClusterCacheStats} instead.
 */
@Scope(Scopes.GLOBAL)
@Deprecated
public interface CacheContainerStats extends Stats {

   String OBJECT_NAME = "CacheContainerStats";

   double getHitRatio();

   double getReadWriteRatio();
}
