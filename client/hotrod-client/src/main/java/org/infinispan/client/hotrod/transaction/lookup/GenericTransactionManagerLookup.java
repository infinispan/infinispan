package org.infinispan.client.hotrod.transaction.lookup;

import java.util.Optional;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.transaction.manager.RemoteTransactionManager;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.tx.lookup.LookupNames;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;

import net.jcip.annotations.GuardedBy;

/**
 * A {@link TransactionManagerLookup} implementation that attempts to locate a {@link TransactionManager}.
 * <p>
 * A variety of different classes and JNDI locations are tried, for servers such as: <ul> <li> JBoss <li> JRun4 <li>
 * Resin <li> Orion <li> JOnAS <li> BEA Weblogic <li> Websphere 4.0, 5.0, 5.1, 6.0 <li> Sun, Glassfish </ul>.
 * <p>
 * If a transaction manager is not found, returns an {@link RemoteTransactionManager}.
 *
 * @author Pedro Ruivo
 * @since 9.3
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

      transactionManager = tryTmFactoryLookup().orElseGet(RemoteTransactionManager::getInstance);
      transactionManager = tryJndiLookup().orElseGet(
            () -> tryTmFactoryLookup().orElseGet(RemoteTransactionManager::getInstance));
      return transactionManager;
   }

   private Optional<TransactionManager> tryJndiLookup() {
      InitialContext ctx;
      try {
         ctx = new InitialContext();
      } catch (NamingException e) {
         return Optional.empty();
      }

      try {
         //probe jndi lookups first
         for (LookupNames.JndiTransactionManager knownJNDIManager : LookupNames.JndiTransactionManager.values()) {
            Object jndiObject;
            try {
               jndiObject = ctx.lookup(knownJNDIManager.getJndiLookup());
            } catch (NamingException e) {
               continue;
            }
            if (jndiObject instanceof TransactionManager) {
               return Optional.of((TransactionManager) jndiObject);
            }
         }
      } finally {
         Util.close(ctx);
      }
      return Optional.empty();
   }

   private Optional<TransactionManager> tryTmFactoryLookup() {
      for (LookupNames.TransactionManagerFactory transactionManagerFactory : LookupNames.TransactionManagerFactory
            .values()) {
         TransactionManager transactionManager = transactionManagerFactory
               .tryLookup(GenericTransactionManagerLookup.class.getClassLoader());
         if (transactionManager != null) {
            return Optional.of(transactionManager);
         }
      }
      return Optional.empty();
   }
}
