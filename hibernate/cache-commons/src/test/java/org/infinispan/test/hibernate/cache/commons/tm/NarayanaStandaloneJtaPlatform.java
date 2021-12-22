package org.infinispan.test.hibernate.cache.commons.tm;


import org.hibernate.engine.transaction.jta.platform.internal.AbstractJtaPlatform;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatformException;

import jakarta.transaction.TransactionManager;
import jakarta.transaction.UserTransaction;

public class NarayanaStandaloneJtaPlatform extends AbstractJtaPlatform {

   public NarayanaStandaloneJtaPlatform() {
   }

   @Override
   protected TransactionManager locateTransactionManager() {
      try {
         return com.arjuna.ats.jta.TransactionManager.transactionManager();
      } catch (Exception e) {
         throw new JtaPlatformException("Could not obtain JBoss Transactions transaction manager instance", e);
      }
   }

   @Override
   protected UserTransaction locateUserTransaction() {
      try {
         return com.arjuna.ats.jta.UserTransaction.userTransaction();
      } catch (Exception e) {
         throw new JtaPlatformException("Could not obtain JBoss Transactions user transaction instance", e);
      }
   }

}
