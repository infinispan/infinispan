package org.infinispan.xsite.status;

import java.util.LinkedList;
import java.util.List;

/**
 * A per-container {@link SiteStatus} builder.
 * <p>
 * It builds a {@link SiteStatus} based on the caches which have the site online, offline or mixed status.
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
public class ContainerSiteStatusBuilder extends AbstractSiteStatusBuilder<String> {

   private final List<String> mixedCaches;

   public ContainerSiteStatusBuilder() {
      super();
      mixedCaches = new LinkedList<>();
   }

   /**
    * Adds the cache with an mixed connection to the site.
    *
    * @param cacheName The cache name.
    */
   public void mixedOn(String cacheName) {
      mixedCaches.add(cacheName);
   }

   /**
    * Adds the cache with the {@link SiteStatus} connection to the site.
    *
    * @param cacheName The cache name.
    * @param status    {@link SiteStatus} of the site.
    */
   public void addCacheName(String cacheName, SiteStatus status) {
      if (status.isOnline()) {
         onlineOn(cacheName);
      } else if (status.isOffline()) {
         offlineOn(cacheName);
      } else {
         mixedOn(cacheName);
      }
   }

   @Override
   protected boolean isOnline() {
      return super.isOnline() && mixedCaches.isEmpty();
   }

   @Override
   protected boolean isOffline() {
      return super.isOffline() && mixedCaches.isEmpty();
   }

   @Override
   protected SiteStatus createMixedStatus(List<String> onlineElements, List<String> offlineElements) {
      return new ContainerMixedSiteStatus(onlineElements, offlineElements, mixedCaches);
   }
}
