package org.infinispan.persistence.support;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.util.function.TriPredicate;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

@Scope(Scopes.NAMED_CACHE)
public class DelegatingPersistenceManager implements PersistenceManager, Lifecycle {
   protected final PersistenceManager persistenceManager;

   public DelegatingPersistenceManager(PersistenceManager persistenceManager) {
      this.persistenceManager = persistenceManager;
   }


   @Inject
   void inject(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(persistenceManager, false);
   }

   @Start
   @Override
   public void start() {
      persistenceManager.start();
   }

   @Stop
   @Override
   public void stop() {
      persistenceManager.stop();
   }

   public PersistenceManager getActual() {
      return persistenceManager;
   }

   @Override
   public boolean isEnabled() {
      return persistenceManager.isEnabled();
   }

   @Override
   public boolean hasWriter() {
      return persistenceManager.hasWriter();
   }

   @Override
   public boolean hasStore(Predicate<StoreConfiguration> test) {
      return persistenceManager.hasStore(test);
   }

   @Override
   public Flowable<MarshallableEntry<Object, Object>> preloadPublisher() {
      return persistenceManager.preloadPublisher();
   }

   @Override
   public CompletionStage<Void> disableStore(String storeType) {
      return persistenceManager.disableStore(storeType);
   }

   @Override
   public CompletionStage<Void> addStore(StoreConfiguration persistenceConfiguration) {
      return persistenceManager.addStore(persistenceConfiguration);
   }

   @Override
   public void addStoreListener(StoreChangeListener listener) {
      persistenceManager.addStoreListener(listener);
   }

   @Override
   public void removeStoreListener(StoreChangeListener listener) {
      persistenceManager.removeStoreListener(listener);
   }

   @Override
   public <T> Set<T> getStores(Class<T> storeClass) {
      return persistenceManager.getStores(storeClass);
   }

   @Override
   public Collection<String> getStoresAsString() {
      return persistenceManager.getStoresAsString();
   }

   @Override
   public CompletionStage<Void> purgeExpired() {
      return persistenceManager.purgeExpired();
   }

   @Override
   public CompletionStage<Void> clearAllStores(Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.clearAllStores(predicate);
   }

   @Override
   public CompletionStage<Boolean> deleteFromAllStores(Object key, int segment, Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.deleteFromAllStores(key, segment, predicate);
   }

   @Override
   public <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue,
                                                                   boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.publishEntries(filter, fetchValue, fetchMetadata, predicate);
   }

   @Override
   public <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter,
                                                                   boolean fetchValue, boolean fetchMetadata,
                                                                   Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.publishEntries(segments, filter, fetchValue, fetchMetadata, predicate);
   }

   @Override
   public <K> Publisher<K> publishKeys(Predicate<? super K> filter, Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.publishKeys(filter, predicate);
   }

   @Override
   public <K> Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter,
                                       Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.publishKeys(segments, filter, predicate);
   }

   @Override
   public <K, V> CompletionStage<MarshallableEntry<K, V>> loadFromAllStores(Object key, boolean localInvocation,
                                                                            boolean includeStores) {
      return persistenceManager.loadFromAllStores(key, localInvocation, includeStores);
   }

   @Override
   public CompletionStage<Long> size(Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.size(predicate);
   }

   @Override
   public void setClearOnStop(boolean clearOnStop) {
      persistenceManager.setClearOnStop(clearOnStop);
   }

   @Override
   public CompletionStage<Void> writeToAllNonTxStores(MarshallableEntry marshalledEntry, int segment,
                                                      Predicate<? super StoreConfiguration> predicate, long flags) {
      return persistenceManager.writeToAllNonTxStores(marshalledEntry, segment, predicate, flags);
   }

   @Override
   public CompletionStage<Void> prepareAllTxStores(TxInvocationContext<AbstractCacheTransaction> txInvocationContext,
                                                   Predicate<? super StoreConfiguration> predicate) throws PersistenceException {
      return persistenceManager.prepareAllTxStores(txInvocationContext, predicate);
   }

   @Override
   public CompletionStage<Void> commitAllTxStores(TxInvocationContext<AbstractCacheTransaction> txInvocationContext,
                                                  Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.commitAllTxStores(txInvocationContext, predicate);
   }

   @Override
   public CompletionStage<Void> rollbackAllTxStores(TxInvocationContext<AbstractCacheTransaction> txInvocationContext,
                                                    Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.rollbackAllTxStores(txInvocationContext, predicate);
   }

   @Override
   public CompletionStage<Long> writeMapCommand(PutMapCommand putMapCommand, InvocationContext ctx,
                                                BiPredicate<? super PutMapCommand, Object> commandKeyPredicate) {
      return persistenceManager.writeMapCommand(putMapCommand, ctx, commandKeyPredicate);
   }

   @Override
   public <K, V> CompletionStage<Void> writeEntries(Iterable<MarshallableEntry<K, V>> iterable,
                                                    Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.writeEntries(iterable, predicate);
   }

   @Override
   public CompletionStage<Long> performBatch(TxInvocationContext<AbstractCacheTransaction> invocationContext,
                                             TriPredicate<? super WriteCommand, Object, MVCCEntry<?, ?>> commandKeyPredicate) {
      return persistenceManager.performBatch(invocationContext, commandKeyPredicate);
   }

   @Override
   public boolean isAvailable() {
      return persistenceManager.isAvailable();
   }

   @Override
   public boolean isReadOnly() {
      return persistenceManager.isReadOnly();
   }

   @Override
   public <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(boolean fetchValue, boolean fetchMetadata) {
      return persistenceManager.publishEntries(fetchValue, fetchMetadata);
   }

   @Override
   public <K, V> CompletionStage<MarshallableEntry<K, V>> loadFromAllStores(Object key, int segment,
                                                                            boolean localInvocation,
                                                                            boolean includeStores) {
      return persistenceManager.loadFromAllStores(key, segment, localInvocation, includeStores);
   }

   @Override
   public CompletionStage<Long> approximateSize(Predicate<? super StoreConfiguration> predicate, IntSet segments) {
      return persistenceManager.approximateSize(predicate, segments);
   }

   @Override
   public CompletionStage<Long> size() {
      return persistenceManager.size();
   }

   @Override
   public CompletionStage<Long> size(Predicate<? super StoreConfiguration> predicate, IntSet segments) {
      return persistenceManager.size(predicate, segments);
   }

   @Override
   public CompletionStage<Void> writeToAllNonTxStores(MarshallableEntry marshalledEntry, int segment,
                                                      Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.writeToAllNonTxStores(marshalledEntry, segment, predicate);
   }

   @Override
   public CompletionStage<Boolean> addSegments(IntSet segments) {
      return persistenceManager.addSegments(segments);
   }

   @Override
   public CompletionStage<Boolean> removeSegments(IntSet segments) {
      return persistenceManager.removeSegments(segments);
   }
}
