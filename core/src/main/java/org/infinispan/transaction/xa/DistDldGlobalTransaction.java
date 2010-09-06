package org.infinispan.transaction.xa;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.remoting.transport.Address;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class DistDldGlobalTransaction extends DldGlobalTransaction {

   private volatile DistributionManager distManager;
   private volatile int numOwners;

   public DistDldGlobalTransaction(DistributionManager distManager, int numOwners) {
      this.distManager = distManager;
      this.numOwners = numOwners;
   }

   public DistDldGlobalTransaction(Address addr, boolean remote, DistributionManager distManager, int numOwners) {
      super(addr, remote);
      this.distManager = distManager;
      this.numOwners = numOwners;
   }

   @Override
   public boolean isAcquiringRemoteLock(Object key, Address address) {
      boolean affectsKey = remoteLockIntention.contains(key);
      if (affectsKey) {
         return distManager.getConsistentHash().isKeyLocalToAddress(address, key, numOwners);
      } else {
         return false;
      }
   }
}
