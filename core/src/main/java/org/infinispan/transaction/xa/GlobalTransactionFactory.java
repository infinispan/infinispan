package org.infinispan.transaction.xa;

import org.infinispan.config.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.transport.Address;

import java.util.Random;

/**
 * Factory for GlobalTransaction/DeadlockDetectingGlobalTransaction.
 *
 * @author Mircea.Markus@jboss.com
 */
public class GlobalTransactionFactory {

   private boolean isEddEnabled = false;

   private DistributionManager distributionManager;

   private Configuration configuration;

   @Inject
   public void init(DistributionManager distributionManager, Configuration configuration) {
      this.distributionManager = distributionManager;
      this.configuration = configuration;
   }

   /** this class is internally synchronized, so it can be shared between instances */
   private final Random rnd = new Random();

   private long generateRandomId() {
      return rnd.nextLong();
   }


   public GlobalTransactionFactory() {
   }

   public GlobalTransactionFactory(boolean eddEnabled) {
      isEddEnabled = eddEnabled;
   }

   @Inject
   public void init(Configuration configuration) {
      isEddEnabled = configuration.isEnableDeadlockDetection();
   }

   @Start
   public void start() {

   }

   public GlobalTransaction instantiateGlobalTransaction() {
      if (isEddEnabled) {
         return new DldGlobalTransaction();
      } else {
         return new GlobalTransaction();
      }
   }

   public GlobalTransaction newGlobalTransaction(Address addr, boolean remote) {
      GlobalTransaction gtx;
      if (isEddEnabled) {
         DldGlobalTransaction globalTransaction;
         globalTransaction = new DldGlobalTransaction(addr, remote);
         globalTransaction.setCoinToss(generateRandomId());
         gtx = globalTransaction;
      } else {
         gtx = new GlobalTransaction(addr, remote);
      }
      return gtx;
   }
}
