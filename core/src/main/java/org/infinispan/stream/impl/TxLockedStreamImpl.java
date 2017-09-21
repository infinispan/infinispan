package org.infinispan.stream.impl;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.commons.CacheException;
import org.infinispan.container.entries.CacheEntry;

/**
 * Locked Stream that is designed for transactions. This way it can suspend and resume the transaction upon invocation.
 * @author wburns
 * @since 9.0
 */
public class TxLockedStreamImpl<K, V> extends LockedStreamImpl<K, V> {
   final TransactionManager tm;

   public TxLockedStreamImpl(TransactionManager tm, CacheStream<CacheEntry<K, V>> realStream, long time, TimeUnit unit) {
      super(realStream, time, unit);
      this.tm = Objects.requireNonNull(tm);
   }

   TxLockedStreamImpl(TransactionManager tm, CacheStream<CacheEntry<K, V>> realStream,
         Predicate<? super CacheEntry<K, V>> predicate, long time, TimeUnit unit) {
      super(realStream, predicate, time, unit);
      this.tm = tm;
   }

   @Override
   public void forEach(BiConsumer<Cache<K, V>, ? super CacheEntry<K, V>> biConsumer) {
      Transaction ongoingTransaction = null;
      try {
         ongoingTransaction = suspendOngoingTransactionIfExists();
         super.forEach(biConsumer);
      } finally {
         resumePreviousOngoingTransaction(ongoingTransaction);
      }
   }

   @Override
   public <R> Map<K, R> invokeAll(BiFunction<Cache<K, V>, ? super CacheEntry<K, V>, R> biFunction) {
      Transaction ongoingTransaction = null;
      try {
         ongoingTransaction = suspendOngoingTransactionIfExists();
         return super.invokeAll(biFunction);
      } finally {
         resumePreviousOngoingTransaction(ongoingTransaction);
      }
   }

   private Transaction suspendOngoingTransactionIfExists() {
      final Transaction tx = getOngoingTransaction();
      if (tx != null) {
         try {
            tm.suspend();
         } catch (SystemException e) {
            throw new CacheException("Unable to suspend transaction.", e);
         }
      }
      return tx;
   }

   private Transaction getOngoingTransaction() {
      try {
         return tm.getTransaction();
      } catch (SystemException e) {
         throw new CacheException("Unable to get transaction", e);
      }
   }

   private void resumePreviousOngoingTransaction(Transaction transaction) {
      if (transaction != null) {
         try {
            tm.resume(transaction);
         } catch (Exception e) {
            throw new CacheException("Had problems trying to resume a transaction after locked stream forEach()", e);
         }
      }
   }

   @Override
   LockedStreamImpl<K, V> newStream(CacheStream<CacheEntry<K, V>> realStream,
         Predicate<? super CacheEntry<K, V>> predicate, long time, TimeUnit unit) {
      return new TxLockedStreamImpl<>(tm, realStream, predicate, time, unit);
   }
}
