package org.infinispan.notifications.cachemanagerlistener.event;

import java.util.List;

import org.infinispan.remoting.transport.Address;

/**
 * This event is passed in to any method annotated with {@link org.infinispan.notifications.cachemanagerlistener.annotation.Merged}.
 *
 * @author Manik Surtani
 * @version 4.2
 */
public interface MergeEvent extends ViewChangedEvent {
   List<List<Address>> getSubgroupsMerged();
}
