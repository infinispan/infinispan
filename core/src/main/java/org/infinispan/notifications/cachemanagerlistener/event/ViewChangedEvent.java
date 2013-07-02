package org.infinispan.notifications.cachemanagerlistener.event;

import org.infinispan.remoting.transport.Address;

import java.util.List;

/**
 * This event is passed in to any method annotated with {@link org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged}.
 * It represents a JGroups view change event.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 4.0
 */
public interface ViewChangedEvent extends Event {

   /**
    * Gets the current list of members.
    *
    * @return the new view associated with this view change. List cannot be null.
    */
   List<Address> getNewMembers();

   /**
    * Gets the previous list of members.
    *
    * @return the old view associated with this view change. List cannot be null.
    */
   List<Address> getOldMembers();

   Address getLocalAddress();

   /**
    * Get JGroups view id.
    * @return
    */
   int getViewId();

   boolean isMergeView();
}
