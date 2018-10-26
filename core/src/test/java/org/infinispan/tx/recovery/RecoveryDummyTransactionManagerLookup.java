package org.infinispan.tx.recovery;

import javax.transaction.TransactionManager;

import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransactionManager;

/**
 * @author Mircea Markus &lt;mircea.markus@jboss.com&gt; (C) 2011 Red Hat Inc.
 * @since 5.1
 * @deprecated use {@link EmbeddedTransactionManagerLookup}
 */
@Deprecated
public class RecoveryDummyTransactionManagerLookup implements TransactionManagerLookup {

   @Override
   public synchronized TransactionManager getTransactionManager() throws Exception {
      DummyTransactionManager dtm = new DummyTransactionManager();
      dtm.setUseXaXid(true);
      return dtm;
   }
}
