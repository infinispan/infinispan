package org.infinispan.partionhandling;

import org.infinispan.Cache;
import org.infinispan.remoting.transport.Address;

import java.util.List;


public interface PartitionContext<K,V> {
   /**
    * returns the list of members before the partition happened.
    */
   List<Address> getOldMembers();

   /**
    * returns the list of members as seen within this partition.
    */
   List<Address> getNewMembers();

   /**
    * Returns true if this partition might not contain all the data present in the cluster before partitioning happened.
    * E.g. if numOwners=5 and only 3 nodes left in the other partition, then this method returns false. If 6 nodes left
    * this method returns true.
    */
   boolean isAllDataAvailable();

   /**
    * Marks the current partition as available or not (writes are rejected with a AvailabilityException).
    */
   void enterDegradedMode();

   /**
    * Invoking this method triggers rebalance.
    */
   void rebalance();

}
