package org.infinispan.client.hotrod.impl.transaction;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.Util;
import org.infinispan.client.hotrod.impl.transaction.entry.TransactionEntry;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.time.TimeService;

/**
 * A {@link RemoteCache} implementation that handles {@link Transaction}.
 * <p>
 * All streaming operation (example {@link TransactionalRemoteCacheImpl#retrieveEntries(String, int)}) and {@link
 * TransactionalRemoteCacheImpl#size()} aren't transactional in a way they don't interact with the transaction's data
 * (keys, values).
 * <p>
 * {@link TransactionalRemoteCacheImpl#containsValue(Object)} is a special case where a key with the specific value is
 * marked as read for the transaction.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public class TransactionalRemoteCacheImpl<K, V> extends RemoteCacheImpl<K, V> {

   private static final Log log = LogFactory.getLog(TransactionalRemoteCacheImpl.class, Log.class);

   private final boolean forceReturnValue;
   private final boolean recoveryEnabled;
   private final TransactionManager transactionManager;
   private final TransactionTable transactionTable;

   // TODO: the remote get is force to be sync and is blocking!
   // https://issues.redhat.com/browse/ISPN-11633
   private final Function<K, MetadataValue<V>> remoteGet = this::getWithMetadataNotTracked;
   private final Function<K, byte[]> keyMarshaller = this::keyToBytes;
   private final Function<V, byte[]> valueMarshaller = this::valueToBytes;

   public TransactionalRemoteCacheImpl(RemoteCacheManager rcm, String name, boolean forceReturnValue,
         boolean recoveryEnabled, TransactionManager transactionManager,
         TransactionTable transactionTable, TimeService timeService) {
      super(rcm, name, timeService);
      this.forceReturnValue = forceReturnValue;
      this.recoveryEnabled = recoveryEnabled;
      this.transactionManager = transactionManager;
      this.transactionTable = transactionTable;
   }

   @Override
   public CompletableFuture<Boolean> removeWithVersionAsync(K key, long version) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.removeWithVersionAsync(key, version) :
            txContext.compute(key, entry -> removeEntryIfSameVersion(entry, version), remoteGet);
   }

   @Override
   public CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, long lifespan,
         TimeUnit lifespanTimeUnit,
         long maxIdle, TimeUnit maxIdleTimeUnit) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.replaceWithVersionAsync(key, newValue, version, lifespan, lifespanTimeUnit, maxIdle,
                  maxIdleTimeUnit) :
            txContext.compute(key,
                  entry -> replaceEntryIfSameVersion(entry, newValue, version, lifespan, lifespanTimeUnit, maxIdle,
                        maxIdleTimeUnit),
                  remoteGet);
   }

   @Override
   public CompletableFuture<MetadataValue<V>> getWithMetadataAsync(K key) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.getWithMetadataAsync(key) :
            txContext.compute(key, TransactionEntry::toMetadataValue, remoteGet);
   }

   private MetadataValue<V> getWithMetadataNotTracked(K key) {
      return Util.await(super.getWithMetadataAsync(key));
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit,
         long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      TransactionContext<K, V> txContext = getTransactionContext();
      if (txContext == null) {
         return super.putAllAsync(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      } else {
         //this is local only
         map.forEach((key, value) ->
               txContext.compute(key,
                     entry -> getAndSetEntry(entry, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit)));
         return CompletableFuture.completedFuture(null);
      }
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      TransactionContext<K, V> txContext = getTransactionContext();
      if (txContext == null) {
         return super.putAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      }
      if (forceReturnValue) {
         return txContext.compute(key,
               entry -> getAndSetEntry(entry, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit),
               remoteGet);
      } else {
         return txContext.compute(key,
               entry -> getAndSetEntry(entry, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
      }
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit) :
            txContext.compute(key,
                  entry -> putEntryIfAbsent(entry, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit),
                  remoteGet);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.replaceAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit) :
            txContext.compute(key,
                  entry -> replaceEntry(entry, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit),
                  remoteGet);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.replaceAsync(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit) :
            txContext.compute(key,
                  entry -> replaceEntryIfEquals(entry, oldValue, value, lifespan, lifespanUnit, maxIdleTime,
                        maxIdleTimeUnit),
                  remoteGet);
   }

   @Override
   public CompletableFuture<Boolean> containsKeyAsync(K key) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.containsKeyAsync(key) :
            txContext.containsKey(key, remoteGet);
   }

   @Override
   public boolean containsValue(Object value) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.containsValue(value) :
            txContext.containsValue(value, super::entrySet, remoteGet);
   }

   @Override
   public CompletableFuture<V> getAsync(Object key) {
      TransactionContext<K, V> txContext = getTransactionContext();
      //noinspection unchecked
      return txContext == null ?
            super.getAsync(key) :
            txContext.compute((K) key, TransactionEntry::getValue, remoteGet);
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      TransactionContext<K, V> txContext = getTransactionContext();
      if (txContext == null) {
         return super.removeAsync(key);
      }
      if (forceReturnValue) {
         //noinspection unchecked
         return txContext.compute((K) key, this::removeEntry, remoteGet);
      } else {
         //noinspection unchecked
         return txContext.compute((K) key, this::removeEntry);
      }
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      TransactionContext<K, V> txContext = getTransactionContext();
      //noinspection unchecked
      return txContext == null ?
            super.removeAsync(key, value) :
            txContext.compute((K) key, entry -> removeEntryIfEquals(entry, value), remoteGet);
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

   private boolean removeEntryIfSameVersion(TransactionEntry<K, V> entry, long version) {
      if (entry.exists() && entry.getVersion() == version) {
         entry.remove();
         return true;
      } else {
         return false;
      }
   }

   private boolean replaceEntryIfSameVersion(TransactionEntry<K, V> entry, V newValue, long version, long lifespan,
         TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      if (entry.exists() && entry.getVersion() == version) {
         entry.set(newValue, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
         return true;
      } else {
         return false;
      }
   }

   private V putEntryIfAbsent(TransactionEntry<K, V> entry, V value, long lifespan, TimeUnit lifespanTimeUnit,
         long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      V currentValue = entry.getValue();
      if (currentValue == null) {
         entry.set(value, lifespan, lifespanTimeUnit, maxIdleTime, maxIdleTimeUnit);
      }
      return currentValue;
   }

   private V replaceEntry(TransactionEntry<K, V> entry, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      V currentValue = entry.getValue();
      if (currentValue != null) {
         entry.set(value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      }
      return currentValue;
   }

   private boolean replaceEntryIfEquals(TransactionEntry<K, V> entry, V oldValue, V value, long lifespan,
         TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      V currentValue = entry.getValue();
      if (currentValue != null && Objects.deepEquals(currentValue, oldValue)) {
         entry.set(value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
         return true;
      } else {
         return false;
      }
   }

   private V removeEntry(TransactionEntry<K, V> entry) {
      V oldValue = entry.getValue();
      entry.remove();
      return oldValue;
   }

   private boolean removeEntryIfEquals(TransactionEntry<K, V> entry, Object value) {
      V oldValue = entry.getValue();
      if (oldValue != null && Objects.deepEquals(oldValue, value)) {
         entry.remove();
         return true;
      }
      return false;
   }

   private V getAndSetEntry(TransactionEntry<K, V> entry, V value, long lifespan, TimeUnit lifespanUnit,
         long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      V oldValue = entry.getValue();
      entry.set(value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return oldValue;
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
