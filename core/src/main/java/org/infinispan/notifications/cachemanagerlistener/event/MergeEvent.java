package org.infinispan.notifications.cachemanagerlistener.event;

import org.infinispan.remoting.transport.Address;

import java.util.List;

/**
 * This event is passed in to any method annotated with {@link org.infinispan.notifications.cachemanagerlistener.annotation.Merged}.
 *
 * @author Manik Surtani
 * @version 4.2
 */
public interface MergeEvent {
   List<List<Address>> getSubgroupsMerged();
}
