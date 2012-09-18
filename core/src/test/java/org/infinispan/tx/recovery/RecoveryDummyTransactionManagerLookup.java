package org.infinispan.tx.recovery;

import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransactionManager;

import javax.transaction.TransactionManager;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.1
 */
public class RecoveryDummyTransactionManagerLookup implements TransactionManagerLookup {

   @Override
   public synchronized TransactionManager getTransactionManager() throws Exception {
      DummyTransactionManager dtm = new DummyTransactionManager();
      dtm.setUseXaXid(true);
      return dtm;
   }
}
