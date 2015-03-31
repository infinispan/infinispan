package org.infinispan.stats;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Similar to {@link Stats} but in the scope of a single per node CacheContainer
 *
 * @author Vladimir Blagojevic
 * @since 7.1
 *
 */
@Scope(Scopes.GLOBAL)
public interface CacheContainerStats extends Stats {
   public static final String OBJECT_NAME = "CacheContainerStats";

   double getHitRatio();

   double getReadWriteRatio();
}
