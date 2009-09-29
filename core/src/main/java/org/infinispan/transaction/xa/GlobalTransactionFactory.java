package org.infinispan.transaction.xa;

import org.infinispan.config.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.transport.Address;

import java.util.Random;

/**
 * Factory for GlobalTransaction/DadlockDetectingGlobalTransaction.
 *
 * @author Mircea.Markus@jboss.com
 */
public class GlobalTransactionFactory {

   private boolean isEddEnabled = false;

   /** this class is internally synchronized, so it can be shared between instances */
   private final Random rnd = new Random();

   private long generateRandomdId() {
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
         return new DeadlockDetectingGlobalTransaction();
      } else {
         return new GlobalTransaction();
      }
   }

   public GlobalTransaction newGlobalTransaction(Address addr, boolean remote) {
      GlobalTransaction gtx;
      if (isEddEnabled) {
         DeadlockDetectingGlobalTransaction globalTransaction = new DeadlockDetectingGlobalTransaction(addr, remote);
         globalTransaction.setCoinToss(generateRandomdId());
         gtx = globalTransaction;
      } else {
         gtx = new GlobalTransaction(addr, remote);
      }

      return gtx;
   }
}
