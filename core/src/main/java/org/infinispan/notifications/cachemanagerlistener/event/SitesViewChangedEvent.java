package org.infinispan.notifications.cachemanagerlistener.event;

import java.util.Collection;

/**
 * This event is passed in to any method annotated with {@link org.infinispan.notifications.cachemanagerlistener.annotation.SiteViewChanged}.
 * It represents a site up or down event.
 *
 * @since 15.0
 */
public interface SitesViewChangedEvent extends Event {

   /**
    * @return The current list of connected sites.
    */
   Collection<String> getSites();

   /**
    * @return The list of new connected sites.
    */
   Collection<String> getJoiners();

   /**
    * @return The list of sites that have disconnected.
    */
   Collection<String> getLeavers();

}
