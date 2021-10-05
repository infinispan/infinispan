package org.infinispan.persistence.manager;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.InvocationHelper;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.transaction.impl.TransactionCoordinator;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

/**
 * Separate the preload into its own component
 */
@Scope(Scopes.NAMED_CACHE)
public class PreloadManager {
   public static final long PRELOAD_FLAGS = FlagBitSets.CACHE_MODE_LOCAL |
                                            FlagBitSets.SKIP_OWNERSHIP_CHECK |
                                            FlagBitSets.IGNORE_RETURN_VALUES |
                                            FlagBitSets.SKIP_CACHE_STORE |
                                            FlagBitSets.SKIP_LOCKING |
                                            FlagBitSets.SKIP_XSITE_BACKUP |
                                            FlagBitSets.IRAC_STATE;
   public static final long PRELOAD_WITHOUT_INDEXING_FLAGS =
         EnumUtil.mergeBitSets(PRELOAD_FLAGS, FlagBitSets.SKIP_INDEXING);

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject Configuration configuration;
   @Inject protected PersistenceManager persistenceManager;
   @Inject TimeService timeService;
   @Inject protected ComponentRef<AdvancedCache<?, ?>> cache;
   @Inject CommandsFactory commandsFactory;
   @Inject KeyPartitioner keyPartitioner;

   @Inject InvocationContextFactory invocationContextFactory;
   @Inject InvocationHelper invocationHelper;
   @Inject TransactionCoordinator transactionCoordinator;
   @Inject TransactionManager transactionManager;
   @Inject BlockingManager blockingManager;
   private volatile boolean fullyPreloaded;

   @Start
   public void start() {
      fullyPreloaded = false;
      CompletionStages.join(doPreload());
   }

   private CompletionStage<Void> doPreload() {
      Publisher<MarshallableEntry<Object, Object>> publisher = persistenceManager.preloadPublisher();

      long start = timeService.time();

      final long maxEntries = getMaxEntries();
      final long flags = getFlagsForStateInsertion();
      AdvancedCache<?,?> tmpCache = this.cache.wired().withStorageMediaType();
      DataConversion keyDataConversion = tmpCache.getKeyDataConversion();
      DataConversion valueDataConversion = tmpCache.getValueDataConversion();

      Transaction outerTransaction = suspendIfNeeded();
      try {
         return Flowable.fromPublisher(publisher)
                        .take(maxEntries)
                        .concatMapSingle(me -> preloadEntry(flags, me, keyDataConversion, valueDataConversion))
                        .count()
                        .toCompletionStage()
                        .thenAccept(insertAmount -> {
                           this.fullyPreloaded = insertAmount < maxEntries;
                           log.debugf("Preloaded %d keys in %s", insertAmount,
                                      Util.prettyPrintTime(timeService.timeDuration(start, MILLISECONDS)));
                        });
      } finally {
         resumeIfNeeded(outerTransaction);
      }
   }

   private Single<?> preloadEntry(long flags, MarshallableEntry<Object, Object> me, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      // CallInterceptor will preserve the timestamps if the metadata is an InternalMetadataImpl instance
      InternalMetadataImpl metadata = new InternalMetadataImpl(me.getMetadata(), me.created(), me.lastUsed());
      // TODO If the storage media type is application/x-protostream, this will convert to POJOs and back
      Object key = keyDataConversion.toStorage(me.getKey());
      Object value = valueDataConversion.toStorage(me.getValue());
      PutKeyValueCommand cmd = commandsFactory.buildPutKeyValueCommand(key, value, keyPartitioner.getSegment(key), metadata, flags);
      cmd.setInternalMetadata(me.getInternalMetadata());

      CompletionStage<?> stage;
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null) {
         final Transaction transaction = suspendIfNeeded();
         CompletionStage<Transaction> putStage;
         try {
            beginIfNeeded();
            putStage = invocationHelper.invokeAsync(cmd, 1)
                                       .thenApply(ignore -> {
                                          try {
                                             return transactionManager.suspend();
                                          } catch (SystemException e) {
                                             throw new PersistenceException("Unable to preload!", e);
                                          }
                                       });
         } catch (Exception e) {
            throw new PersistenceException("Unable to preload!", e);
         }
         stage = blockingManager.whenCompleteBlocking(putStage, (pendingTransaction, t) -> {
            try {
               transactionManager.resume(pendingTransaction);
               commitIfNeeded(t == null);
            } catch (InvalidTransactionException | SystemException e) {
               throw new PersistenceException("Unable to preload!", e);
            } finally {
               resumeIfNeeded(transaction);
            }
         }, me.getKey());
      } else {
         stage = invocationHelper.invokeAsync(cmd, 1);
      }
      // The return value doesn't matter, but it cannot be null
      return Completable.fromCompletionStage(stage).toSingleDefault(me);
   }

   private void resumeIfNeeded(Transaction transaction) {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null &&
          transaction != null) {
         try {
            transactionManager.resume(transaction);
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }
   }

   private Transaction suspendIfNeeded() {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null) {
         try {
            return transactionManager.suspend();
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }
      return null;
   }

   private void beginIfNeeded() throws SystemException, NotSupportedException {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null) {
         transactionManager.begin();
      }
   }

   private void commitIfNeeded(boolean success) {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null) {
         try {
            if (success) {
               transactionManager.commit();
            } else {
               transactionManager.rollback();
            }
         } catch (Exception e) {
            throw new PersistenceException(e);}
      }
   }

   private long getMaxEntries() {
      long maxCount;
      if (configuration.memory().isEvictionEnabled() && (maxCount = configuration.memory().maxCount()) > 0) {
         return maxCount;
      }
      return Long.MAX_VALUE;
   }

   private long getFlagsForStateInsertion() {
      boolean hasSharedStore = persistenceManager.hasStore(StoreConfiguration::shared);
      if (!hasSharedStore  || !configuration.indexing().isVolatile()) {
         return PRELOAD_WITHOUT_INDEXING_FLAGS;
      } else {
         return PRELOAD_FLAGS;
      }
   }

   /**
    * @return true if all entries from the store have been inserted to the cache. If the persistence/preload
    * is disabled or eviction limit was reached when preloading, returns false.
    */
   public boolean isFullyPreloaded() {
      return fullyPreloaded;
   }
}
