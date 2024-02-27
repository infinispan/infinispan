package org.infinispan.hotrod.transaction.lookup;

import org.infinispan.commons.tx.lookup.LookupNames;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.hotrod.transaction.manager.RemoteTransactionManager;

import jakarta.transaction.TransactionManager;
import net.jcip.annotations.GuardedBy;

/**
 * A {@link TransactionManagerLookup} implementation that attempts to locate a {@link TransactionManager}.
 * <p>
 * A variety of different classes and JNDI locations are tried, for servers such as: <ul> <li> JBoss <li> JRun4 <li>
 * Resin <li> Orion <li> JOnAS <li> BEA Weblogic <li> Websphere 4.0, 5.0, 5.1, 6.0 <li> Sun, Glassfish </ul>.
 * <p>
 * If a transaction manager is not found, returns an {@link RemoteTransactionManager}.
 *
 * @since 14.0
 */
public class GenericTransactionManagerLookup implements TransactionManagerLookup {

   private static final GenericTransactionManagerLookup INSTANCE = new GenericTransactionManagerLookup();
   @GuardedBy("this")
   private TransactionManager transactionManager = null;

   private GenericTransactionManagerLookup() {
   }

   public static GenericTransactionManagerLookup getInstance() {
      return INSTANCE;
   }

   @Override
   public synchronized TransactionManager getTransactionManager() {
      if (transactionManager != null) {
         return transactionManager;
      }

      transactionManager = LookupNames.lookupKnownTransactionManagers(GenericTransactionManagerLookup.class.getClassLoader())
            .orElseGet(RemoteTransactionManager::getInstance);
      return transactionManager;
   }
}
