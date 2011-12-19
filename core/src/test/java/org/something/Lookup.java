package org.something;

import javax.transaction.TransactionManager;

import org.infinispan.transaction.lookup.TransactionManagerLookup;

public class Lookup implements TransactionManagerLookup {

   @Override
   public TransactionManager getTransactionManager() throws Exception {
      throw new UnsupportedOperationException();
   }

}
