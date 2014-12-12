package org.infinispan.stats;

/**
 * Similar to {@link Stats} but in the scope of a single per node CacheContainer
 *
 * @author Vladimir Blagojevic
 * @since 7.1
 *
 */
public interface CacheContainerStats extends Stats {
   public static final String OBJECT_NAME = "CacheContainerStats";

   double getHitRatio();

   double getReadWriteRatio();
}
