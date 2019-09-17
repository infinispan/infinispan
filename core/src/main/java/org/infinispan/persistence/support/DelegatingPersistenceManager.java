package org.infinispan.persistence.support;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import javax.transaction.Transaction;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.reactivestreams.Publisher;

@Scope(Scopes.NAMED_CACHE)
public class DelegatingPersistenceManager implements PersistenceManager, Lifecycle {
   protected final PersistenceManager persistenceManager;

   @Inject
   protected ComponentRegistry componentRegistry;

   public DelegatingPersistenceManager(PersistenceManager persistenceManager) {
      this.persistenceManager = persistenceManager;
   }

   @Start
   @Override
   public void start() {
      componentRegistry.wireDependencies(persistenceManager);
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
   public boolean isPreloaded() {
      return persistenceManager.isPreloaded();
   }

   @Override
   public CompletionStage<Void> preload() {
      return persistenceManager.preload();
   }

   @Override
   public void disableStore(String storeType) {
      persistenceManager.disableStore(storeType);
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
   public void purgeExpired() {
      persistenceManager.purgeExpired();
   }

   @Override
   public CompletionStage<Void> clearAllStores(Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.clearAllStores(predicate);
   }

   @Override
   public boolean deleteFromAllStoresSync(Object key, int segment, Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.deleteFromAllStoresSync(key, segment, predicate);
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
   public CompletionStage<Integer> size(Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.size(predicate);
   }

   @Override
   public CompletionStage<Integer> size(IntSet segments) {
      return persistenceManager.size(segments);
   }

   @Override
   public void setClearOnStop(boolean clearOnStop) {
      persistenceManager.setClearOnStop(clearOnStop);
   }

   @Override
   public void writeToAllNonTxStoresSync(MarshallableEntry marshalledEntry, int segment,
                                         Predicate<? super StoreConfiguration> predicate) {
      persistenceManager.writeToAllNonTxStoresSync(marshalledEntry, segment, predicate);
   }

   @Override
   public CompletionStage<Void> writeToAllNonTxStores(MarshallableEntry marshalledEntry, int segment,
                                                      Predicate<? super StoreConfiguration> predicate, long flags) {
      return persistenceManager.writeToAllNonTxStores(marshalledEntry, segment, predicate, flags);
   }

   @Override
   public CompletionStage<Void> prepareAllTxStores(Transaction transaction, BatchModification batchModification,
                                                   Predicate<? super StoreConfiguration> predicate) throws PersistenceException {
      return persistenceManager.prepareAllTxStores(transaction, batchModification, predicate);
   }

   @Override
   public CompletionStage<Void> commitAllTxStores(Transaction transaction, Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.commitAllTxStores(transaction, predicate);
   }

   @Override
   public CompletionStage<Void> rollbackAllTxStores(Transaction transaction, Predicate<? super StoreConfiguration> predicate) {
      return persistenceManager.rollbackAllTxStores(transaction, predicate);
   }

   @Override
   public CompletionStage<Void> writeBatchToAllNonTxStores(Iterable<MarshallableEntry> entries,
                                                           Predicate<? super StoreConfiguration> predicate, long flags) {
      return persistenceManager.writeBatchToAllNonTxStores(entries, predicate, flags);
   }

   @Override
   public CompletionStage<Void> deleteBatchFromAllNonTxStores(Iterable<Object> keys,
                                                              Predicate<? super StoreConfiguration> predicate, long flags) {
      return persistenceManager.deleteBatchFromAllNonTxStores(keys, predicate, flags);
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
   public <K, V> MarshallableEntry<K, V> loadFromAllStoresSync(Object key, boolean localInvocation, boolean includeStores) {
      return persistenceManager.loadFromAllStoresSync(key, localInvocation, includeStores);
   }

   @Override
   public <K, V> CompletionStage<MarshallableEntry<K, V>> loadFromAllStores(Object key, int segment,
                                                                            boolean localInvocation,
                                                                            boolean includeStores) {
      return persistenceManager.loadFromAllStores(key, segment, localInvocation, includeStores);
   }

   @Override
   public <K, V> MarshallableEntry<K, V> loadFromAllStoresSync(Object key, int segment, boolean localInvocation,
                                                               boolean includeStores) {
      return persistenceManager.loadFromAllStoresSync(key, segment, localInvocation, includeStores);
   }

   @Override
   public CompletionStage<Integer> size() {
      return persistenceManager.size();
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
