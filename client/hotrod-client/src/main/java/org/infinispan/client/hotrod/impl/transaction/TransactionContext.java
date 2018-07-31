package org.infinispan.client.hotrod.impl.transaction;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.impl.operations.CompleteTransactionOperation;
import org.infinispan.client.hotrod.impl.operations.ForgetTransactionOperation;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.operations.PrepareTransactionOperation;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transaction.entry.TransactionEntry;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorSet;

/**
 * A context with the keys involved in a {@link Transaction}.
 * <p>
 * There is a single context for each ({@link TransactionalRemoteCacheImpl}, {@link Transaction}) pair.
 * <p>
 * It keeps the keys read and written in order to maintain the transactions isolated.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public class TransactionContext<K, V> {

   private static final Log log = LogFactory.getLog(TransactionContext.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Map<WrappedKey<K>, TransactionEntry<K, V>> entries;
   private final Function<K, byte[]> keyMarshaller;
   private final Function<V, byte[]> valueMarshaller;
   private final OperationsFactory operationsFactory;
   private final String cacheName;

   TransactionContext(Function<K, byte[]> keyMarshaller, Function<V, byte[]> valueMarshaller,
         OperationsFactory operationsFactory, String cacheName) {
      this.keyMarshaller = keyMarshaller;
      this.valueMarshaller = valueMarshaller;
      this.operationsFactory = operationsFactory;
      this.cacheName = cacheName;
      entries = new ConcurrentHashMap<>();
   }

   @Override
   public String toString() {
      return "TransactionContext{" +
            "cacheName='" + cacheName + '\'' +
            ", context-size=" + entries.size() + " (entries)" +
            '}';
   }

   boolean containsKey(Object key, Function<K, MetadataValue<V>> remoteValueSupplier) {
      ByRef<Boolean> result = new ByRef<>(null);
      //noinspection unchecked
      entries.compute(wrap((K) key), (wKey, entry) -> {
         if (entry == null) {
            entry = createEntryFromRemote(wKey.key, remoteValueSupplier);
         }
         result.set(!entry.isNonExists());
         return entry;
      });
      return result.get();
   }

   boolean containsValue(Object value, Supplier<CloseableIteratorSet<Map.Entry<K, V>>> iteratorSupplier,
         Function<K, MetadataValue<V>> remoteValueSupplier) {
      boolean found = entries.values().stream()
            .map(TransactionEntry::getValue)
            .filter(Objects::nonNull)
            .anyMatch(v -> Objects.deepEquals(v, value));
      return found || searchValue(value, iteratorSupplier.get(), remoteValueSupplier);
   }

   <T> CompletableFuture<T> compute(K key, Function<TransactionEntry<K, V>, T> function) {
      CompletableFuture<T> future = new CompletableFuture<>();
      entries.compute(wrap(key), (wKey, entry) -> {
         if (entry == null) {
            entry = TransactionEntry.notReadEntry(wKey.key);
         }
         if (trace) {
            log.tracef("Compute key (%s). Before=%s", wKey, entry);
         }
         T result = function.apply(entry);
         future.complete(result);
         if (trace) {
            log.tracef("Compute key (%s). After=%s (result=%s)", wKey, entry, result);
         }
         return entry;
      });
      return future;
   }

   OperationsFactory getOperationsFactory() {
      return operationsFactory;
   }

   String getCacheName() {
      return cacheName;
   }

   <T> CompletableFuture<T> compute(K key, Function<TransactionEntry<K, V>, T> function,
         Function<K, MetadataValue<V>> remoteValueSupplier) {
      CompletableFuture<T> future = new CompletableFuture<>();
      entries.compute(wrap(key), (wKey, entry) -> {
         if (entry == null) {
            entry = createEntryFromRemote(wKey.key, remoteValueSupplier);
            if (trace) {
               log.tracef("Fetched key (%s) from remote. Entry=%s", wKey, entry);
            }
         }
         if (trace) {
            log.tracef("Compute key (%s). Before=%s", wKey, entry);
         }
         T result = function.apply(entry);
         future.complete(result);
         if (trace) {
            log.tracef("Compute key (%s). After=%s (result=%s)", wKey, entry, result);
         }
         return entry;
      });
      return future;
   }

   boolean isReadWrite() {
      return entries.values().stream().anyMatch(TransactionEntry::isModified);
   }

   Collection<Modification> toModification() {
      return entries.values().stream()
            .filter(TransactionEntry::isModified)
            .map(entry -> entry.toModification(keyMarshaller, valueMarshaller))
            .collect(Collectors.toList());
   }

   <T> T computeSync(K key, Function<TransactionEntry<K, V>, T> function,
         Function<K, MetadataValue<V>> remoteValueSupplier) {
      ByRef<T> ref = new ByRef<>(null);
      entries.compute(wrap(key), (wKey, entry) -> {
         if (entry == null) {
            entry = createEntryFromRemote(wKey.key, remoteValueSupplier);
            if (trace) {
               log.tracef("Fetched key (%s) from remote. Entry=%s", wKey, entry);
            }
         }
         if (trace) {
            log.tracef("Compute key (%s). Before=%s", wKey, entry);
         }
         T result = function.apply(entry);
         ref.set(result);
         if (trace) {
            log.tracef("Compute key (%s). After=%s (result=%s)", wKey, entry, result);
         }
         return entry;
      });
      return ref.get();
   }

   /**
    * Prepares the {@link Transaction} in the server and returns the {@link XAResource} code.
    * <p>
    * A special value {@link Integer#MIN_VALUE} is used to signal an error before contacting the server (for example,
    * it wasn't able to marshall the key/value)
    */
   int prepareContext(Xid xid, boolean onePhaseCommit) {
      PrepareTransactionOperation operation;
      Collection<Modification> modifications;
      try {
         modifications = toModification();
         if (trace) {
            log.tracef("Preparing transaction xid=%s, remote-cache=%s, modification-size=%d", xid, cacheName, modifications.size());
         }
         if (modifications.isEmpty()) {
            return XAResource.XA_RDONLY;
         }
      } catch (Exception e) {
         return Integer.MIN_VALUE;
      }
      try {
         int xaReturnCode;
         do {
            operation = operationsFactory.newPrepareTransactionOperation(xid, onePhaseCommit, modifications);
            xaReturnCode = operation.execute().get();
         } while (operation.shouldRetry());
         return xaReturnCode;
      } catch (Exception e) {
         log.exceptionDuringPrepare(xid, e);
         return XAException.XA_RBROLLBACK;
      }
   }

   int complete(Xid xid, boolean commit) {
      try {
         if (trace) {
            log.tracef("%s transaction xid=%s, remote-cache=%s", commit ? "Committing" : "Rolling-Back", xid, cacheName);
         }
         CompleteTransactionOperation operation = operationsFactory.newCompleteTransactionOperation(xid, commit);
         return operation.execute().get();
      } catch (Exception e) {
         log.debug("Exception while commit/rollback.", e);
         return XAException.XA_HEURRB; //heuristically rolled-back
      }
   }

   void forget(Xid xid) {
      try {
         if (trace) {
            log.tracef("Forgetting transaction xid=%s, remote-cache=%s", xid, cacheName);
         }
         ForgetTransactionOperation operation = operationsFactory.newForgetTransactionOperation(xid);
         operation.execute().get();
      } catch (Exception e) {
         if (trace) {
            log.tracef(e, "Exception in forget transaction xid=%s", xid);
         }
      }
   }

   private TransactionEntry<K, V> createEntryFromRemote(K key, Function<K, MetadataValue<V>> remoteValueSupplier) {
      MetadataValue<V> remoteValue = remoteValueSupplier.apply(key);
      return remoteValue == null ? TransactionEntry.nonExistingEntry(key) : TransactionEntry.read(key, remoteValue);
   }

   private boolean searchValue(Object value, CloseableIteratorSet<Map.Entry<K, V>> iterator,
         Function<K, MetadataValue<V>> remoteValueSupplier) {
      try (CloseableIterator<Map.Entry<K, V>> it = iterator.iterator()) {
         while (it.hasNext()) {
            Map.Entry<K, V> entry = it.next();
            if (!entries.containsKey(wrap(entry.getKey())) && Objects.deepEquals(entry.getValue(), value)) {
               ByRef.Boolean result = new ByRef.Boolean(false);
               entries.computeIfAbsent(wrap(entry.getKey()), wrappedKey -> {
                  MetadataValue<V> remoteValue = remoteValueSupplier.apply(wrappedKey.key);
                  if (Objects.deepEquals(remoteValue.getValue(), value)) {
                     //value didn't change. store it locally.
                     result.set(true);
                     return TransactionEntry.read(wrappedKey.key, remoteValue);
                  } else {
                     return null;
                  }
               });
               if (result.get()) {
                  return true;
               }
            }
         }
      }
      //we iterated over all keys.
      return false;
   }

   private WrappedKey<K> wrap(K key) {
      return new WrappedKey<>(key);
   }

   private static class WrappedKey<K> {
      private final K key;

      private WrappedKey(K key) {
         this.key = key;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) {
            return true;
         }
         if (o == null || getClass() != o.getClass()) {
            return false;
         }

         WrappedKey<?> that = (WrappedKey<?>) o;

         return Objects.deepEquals(key, that.key);
      }

      @Override
      public int hashCode() {
         if (key instanceof Object[]) {
            return Arrays.deepHashCode((Object[]) key);
         } else if (key instanceof byte[]) {
            return Arrays.hashCode((byte[]) key);
         } else if (key instanceof short[]) {
            return Arrays.hashCode((short[]) key);
         } else if (key instanceof int[]) {
            return Arrays.hashCode((int[]) key);
         } else if (key instanceof long[]) {
            return Arrays.hashCode((long[]) key);
         } else if (key instanceof char[]) {
            return Arrays.hashCode((char[]) key);
         } else if (key instanceof float[]) {
            return Arrays.hashCode((float[]) key);
         } else if (key instanceof double[]) {
            return Arrays.hashCode((double[]) key);
         } else if (key instanceof boolean[]) {
            return Arrays.hashCode((boolean[]) key);
         } else {
            return Objects.hashCode(key);
         }
      }

      @Override
      public String toString() {
         return "WrappedKey{" +
               "key=" + toStr(key) +
               '}';
      }
   }

}
