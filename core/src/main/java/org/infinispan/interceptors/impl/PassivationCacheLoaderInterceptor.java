package org.infinispan.interceptors.impl;

import static org.infinispan.factories.KnownComponentNames.PERSISTENCE_EXECUTOR;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class PassivationCacheLoaderInterceptor<K, V> extends CacheLoaderInterceptor<K, V> {
   private static final Log log = LogFactory.getLog(PassivationCacheLoaderInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject @ComponentName(PERSISTENCE_EXECUTOR)
   ExecutorService persistenceExecutor;

   static <K, V> CompletionStage<Void> asyncLoad(ExecutorService persistenceExecutor, CacheLoaderInterceptor<K, V> interceptor,
         Object key, FlagAffectedCommand cmd, InvocationContext ctx) {
      CompletableFuture<Void> loadedFuture = new CompletableFuture<>();
      persistenceExecutor.execute(() -> {
         final AtomicReference<Boolean> isLoaded = new AtomicReference<>();
         InternalCacheEntry entry = PersistenceUtil.loadAndStoreInDataContainer((DataContainer) interceptor.dataContainer,
               SegmentSpecificCommand.extractSegment(cmd, key, interceptor.partitioner), interceptor.persistenceManager, key, ctx, interceptor.timeService,
               isLoaded);
         Boolean isLoadedValue = isLoaded.get();
         if (trace) {
            log.tracef("Entry was loaded? %s", isLoadedValue);
         }
         if (interceptor.getStatisticsEnabled()) {
            if (isLoadedValue == null) {
               // the entry was in data container, we haven't touched cache store
            } else if (isLoadedValue) {
               interceptor.cacheLoads.incrementAndGet();
            } else {
               interceptor.cacheMisses.incrementAndGet();
            }
         }

         CompletionStage<Void> stage = null;
         if (entry != null) {
            interceptor.entryFactory.wrapExternalEntry(ctx, key, entry, true, cmd instanceof WriteCommand);

            if (isLoadedValue != null && isLoadedValue) {
               Object value = entry.getValue();
               // FIXME: There's no point to trigger the entryLoaded/Activated event twice.
               stage = interceptor.sendNotification(key, value, true, ctx, cmd);
               if (CompletionStages.isCompletedSuccessfully(stage)) {
                  stage = interceptor.sendNotification(key, value, false, ctx, cmd);
               } else {
                  stage = stage.thenCompose(v -> interceptor.sendNotification(key, value, false, ctx, cmd));
               }
            }
         }
         CacheEntry contextEntry = ctx.lookupEntry(key);
         if (contextEntry instanceof MVCCEntry) {
            ((MVCCEntry) contextEntry).setLoaded(true);
         }
         if (stage == null) {
            interceptor.cpuExecutor.execute(() -> loadedFuture.complete(null));
         } else {
            stage.whenCompleteAsync((v, t) -> {
               if (t != null) {
                  loadedFuture.completeExceptionally(t);
               } else {
                  loadedFuture.complete(v);
               }
            }, interceptor.cpuExecutor);
         }
      });
      return loadedFuture;
   }

   @Override
   protected CompletionStage<Void> loadInContext(InvocationContext ctx, Object key, FlagAffectedCommand cmd) {
      return asyncLoad(persistenceExecutor, this, key, cmd, ctx);
   }
}
