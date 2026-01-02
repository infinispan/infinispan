package org.infinispan.test.tx;

import org.infinispan.commons.tx.lookup.TransactionManagerLookup;

import jakarta.transaction.TransactionManager;

public class TestLookup implements TransactionManagerLookup {

   @Override
   public TransactionManager getTransactionManager() throws Exception {
      throw new UnsupportedOperationException();
   }

}
