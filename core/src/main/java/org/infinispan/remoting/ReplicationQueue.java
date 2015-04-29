package org.infinispan.remoting;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.api.Lifecycle;

/**
 * Periodically (or when certain size is exceeded) takes elements and replicates them.
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a>
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface ReplicationQueue extends Lifecycle {


   /**
    * @return true if this replication queue is enabled, false otherwise.
    */
   boolean isEnabled();

   /**
    * Adds a new command to the replication queue.
    *
    * @param job command to add to the queue
    */
   void add(ReplicableCommand job);

   /**
    * Flushes existing jobs in the replication queue, and returns the number of jobs flushed.
    * @return the number of jobs flushed
    */
   int flush();

   /**
    * @return the number of elements in the replication queue.
    */
   int getElementsCount();

   /**
    * Resets the replication queue, typically used when a cache is restarted.
    */
   void reset();
}
