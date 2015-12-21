package org.infinispan.server.eventlogger;

import java.util.List;

import org.infinispan.util.logging.events.EventLog;

/**
 * ServerEventLogManager. This global component takes care of maintaining the server event log cache and
 * provides methods for querying its contents across all nodes. For resilience, the event log is
 * stored in a local, bounded, persistent cache and distributed executors are used to gather logs
 * from all the nodes in the cluster.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public interface ServerEventLogManager {

   /**
    * Returns events
    *
    * @param start
    * @param count
    * @return
    */
   List<EventLog> getEvents(int start, int count);

}
