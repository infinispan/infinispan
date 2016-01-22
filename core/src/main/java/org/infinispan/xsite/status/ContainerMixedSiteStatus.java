package org.infinispan.xsite.status;

import java.util.Collection;
import java.util.List;

/**
 * A mixed {@link SiteStatus}.
 *
 * Used per container and it describes the caches in which the site is online, offline and mixed.
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
public class ContainerMixedSiteStatus extends AbstractMixedSiteStatus<String> {

   private final List<String> mixedCaches;

   public ContainerMixedSiteStatus(Collection<String> onlineCacheNameCollection,
                                   Collection<String> offlineCacheNameCollection,
                                   Collection<String> mixedCacheNameCollection) {
      super(onlineCacheNameCollection, offlineCacheNameCollection);
      this.mixedCaches = toImmutable(mixedCacheNameCollection);
   }

   public List<String> getMixedCaches() {
      return mixedCaches;
   }
}
