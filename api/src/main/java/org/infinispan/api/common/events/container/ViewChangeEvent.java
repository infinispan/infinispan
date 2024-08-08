package org.infinispan.api.common.events.container;

import java.util.List;

/**
 * @since 15.0
 **/
public interface ViewChangeEvent extends ContainerEvent {
   /**
    * Gets the current list of members.
    *
    * @return the new view associated with this view change. List cannot be null.
    */
   List<Address> newMembers();

   /**
    * Gets the previous list of members.
    *
    * @return the old view associated with this view change. List cannot be null.
    */
   List<Address> oldMembers();

   Address localAddress();

   int viewId();

   boolean isMergeView();
}
