package org.infinispan.persistence.manager;

import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.support.DelegatingPersistenceManager;
import org.infinispan.telemetry.InfinispanSpan;
import org.infinispan.telemetry.InfinispanSpanAttributes;
import org.infinispan.telemetry.InfinispanTelemetry;
import org.infinispan.telemetry.SpanCategory;
import org.infinispan.telemetry.impl.CacheSpanAttribute;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.util.function.TriPredicate;

@Scope(Scopes.NAMED_CACHE)
public class TracedPersistenceManager extends DelegatingPersistenceManager {

   @Inject
   InfinispanTelemetry telemetryService;

   @Inject
   ComponentRef<AdvancedCache<Object, Object>> cache;

   @Inject
   Configuration configuration;

   private InfinispanSpanAttributes spanAttributes;

   public TracedPersistenceManager(PersistenceManager persistenceManager) {
      super(persistenceManager);
   }

   @Inject
   void cacheSpanAttributes(CacheSpanAttribute cacheSpanAttribute) {
      spanAttributes = cacheSpanAttribute.getAttributes(SpanCategory.PERSISTENCE);
   }

   @Override
   public CompletionStage<Void> purgeExpired() {
      return decorateCommand("purgeExpired", super::purgeExpired);
   }

   @Override
   public CompletionStage<Void> clearAllStores(Predicate<? super StoreConfiguration> predicate) {
      return decorateCommand("clearAllStores", () -> super.clearAllStores(predicate));
   }

   @Override
   public CompletionStage<Boolean> deleteFromAllStores(Object key, int segment,
                                                       Predicate<? super StoreConfiguration> predicate) {
      return decorateCommand("deleteFromAllStores", () -> super.deleteFromAllStores(key, segment, predicate));
   }

   @Override
   public <K, V> CompletionStage<MarshallableEntry<K, V>> loadFromAllStores(Object key, int segment, boolean localInvocation, boolean includeStores) {
      return decorateCommand("loadFromAllStores", () -> super.loadFromAllStores(key, segment, localInvocation, includeStores));
   }

   @Override
   public CompletionStage<Long> approximateSize(Predicate<? super StoreConfiguration> predicate, IntSet segments) {
      return decorateCommand("approximateSize", () -> super.approximateSize(predicate, segments));
   }

   @Override
   public CompletionStage<Long> size(Predicate<? super StoreConfiguration> predicate, IntSet segments) {
      return decorateCommand("size", () -> super.size(predicate, segments));
   }

   @Override
   public CompletionStage<Void> writeToAllNonTxStores(MarshallableEntry marshalledEntry, int segment,
                                                      Predicate<? super StoreConfiguration> predicate, long flags) {
      return decorateCommand("writeToAllNonTxStores", () -> super.writeToAllNonTxStores(marshalledEntry, segment, predicate, flags));
   }

   @Override
   public CompletionStage<Void> commitAllTxStores(TxInvocationContext<AbstractCacheTransaction> txInvocationContext,
                                                  Predicate<? super StoreConfiguration> predicate) {
      return decorateCommand("commitAllTxStores", () -> super.commitAllTxStores(txInvocationContext, predicate));
   }

   @Override
   public CompletionStage<Void> rollbackAllTxStores(TxInvocationContext<AbstractCacheTransaction> txInvocationContext,
                                                    Predicate<? super StoreConfiguration> predicate) {
      return decorateCommand("rollbackAllTxStores", () -> super.rollbackAllTxStores(txInvocationContext, predicate));
   }

   @Override
   public <K, V> CompletionStage<Void> writeEntries(Iterable<MarshallableEntry<K, V>> iterable,
                                                    Predicate<? super StoreConfiguration> predicate) {
      return decorateCommand("writeEntries", () -> super.writeEntries(iterable, predicate));
   }

   @Override
   public CompletionStage<Long> performBatch(TxInvocationContext<AbstractCacheTransaction> ctx,
                                             TriPredicate<? super WriteCommand, Object, MVCCEntry<?, ?>> commandKeyPredicate) {
      return decorateCommand("performBatch", () -> super.performBatch(ctx, commandKeyPredicate));
   }

   private InfinispanSpan<Object> requestStart(String operationName) {
      return telemetryService.startTraceRequest(operationName, spanAttributes);
   }

   private <T> CompletionStage<T> decorateCommand(String operationName, Supplier<CompletionStage<T>> supplier) {
      InfinispanSpan<Object> span = null;
      boolean enabled = configuration.tracing().enabled(SpanCategory.PERSISTENCE);

      if (enabled) {
         span = requestStart(operationName);
      }
      CompletionStage<T> operation = supplier.get();
      if (enabled) {
         operation.whenComplete(span);
      }

      return operation;
   }
}
