package org.infinispan.conflict.impl;

import org.infinispan.conflict.ConflictManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.topology.CacheTopology;

/**
 * @author Ryan Emerson
 * @since 9.1
 */
@Scope(Scopes.NAMED_CACHE)
public interface InternalConflictManager<K, V> extends ConflictManager<K, V> {
   void onTopologyUpdate(LocalizedCacheTopology cacheTopology);
   void cancelVersionRequests();
   void restartVersionRequests();
   void resolveConflicts(CacheTopology cacheTopology);
}
