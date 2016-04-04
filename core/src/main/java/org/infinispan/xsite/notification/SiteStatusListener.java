package org.infinispan.xsite.notification;

/**
 * A simple interface that is invoked by {@link org.infinispan.xsite.OfflineStatus} when a particular site changes its
 * status to online/offline.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface SiteStatusListener {

   default void siteOnline() {
   }

   default void siteOffline() {
   }

}
