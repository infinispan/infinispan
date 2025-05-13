package org.infinispan.stats;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Similar to {@link Stats} but in the scope of a single per-node CacheContainer.
 *
 * @author Vladimir Blagojevic
 * @since 7.1
 */
@Scope(Scopes.GLOBAL)
public interface CacheContainerStats {
   String OBJECT_NAME = "CacheContainerStats";

   int getRequiredMinimumNumberOfNodes();

   long getDataMemoryUsed();

   long getOffHeapMemoryUsed();

   void reset();

   Json toJson();
}
