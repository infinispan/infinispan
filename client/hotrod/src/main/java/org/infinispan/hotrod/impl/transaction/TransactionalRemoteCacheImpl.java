package org.infinispan.hotrod.impl.transaction;

import java.util.function.Function;

import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import org.infinispan.commons.time.TimeService;
import org.infinispan.hotrod.impl.HotRodTransport;
import org.infinispan.hotrod.impl.cache.RemoteCacheImpl;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;

public class TransactionalRemoteCacheImpl<K, V> extends RemoteCacheImpl<K, V> {

   private static final Log log = LogFactory.getLog(TransactionalRemoteCacheImpl.class, Log.class);

   private final boolean recoveryEnabled;
   private final TransactionManager transactionManager;
   private final TransactionTable transactionTable;

   private final Function<K, byte[]> keyMarshaller = this::keyToBytes;
   private final Function<V, byte[]> valueMarshaller = this::valueToBytes;

   public TransactionalRemoteCacheImpl(HotRodTransport hotRodTransport, String name,
                                       boolean recoveryEnabled, TransactionManager transactionManager,
                                       TransactionTable transactionTable, TimeService timeService) {
      super(hotRodTransport, name, timeService, null);
      this.recoveryEnabled = recoveryEnabled;
      this.transactionManager = transactionManager;
      this.transactionTable = transactionTable;
   }

   @Override
   public TransactionManager getTransactionManager() {
      return transactionManager;
   }

   @Override
   public boolean isTransactional() {
      return true;
   }

   boolean isRecoveryEnabled() {
      return recoveryEnabled;
   }

   Function<K, byte[]> keyMarshaller() {
      return keyMarshaller;
   }

   Function<V, byte[]> valueMarshaller() {
      return valueMarshaller;
   }

   private TransactionContext<K, V> getTransactionContext() {
      assertRemoteCacheManagerIsStarted();
      Transaction tx = getRunningTransaction();
      if (tx != null) {
         return transactionTable.enlist(this, tx);
      }
      return null;
   }

   private Transaction getRunningTransaction() {
      try {
         return transactionManager.getTransaction();
      } catch (SystemException e) {
         log.debug("Exception in getRunningTransaction().", e);
         return null;
      }
   }
}
