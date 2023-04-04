package org.infinispan.test.tx;

import jakarta.transaction.TransactionManager;

import org.infinispan.commons.tx.lookup.TransactionManagerLookup;

public class TestLookup implements TransactionManagerLookup {

   @Override
   public TransactionManager getTransactionManager() throws Exception {
      throw new UnsupportedOperationException();
   }

}
