package org.infinispan.persistence.manager;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import javax.transaction.Transaction;

import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.BatchModification;
import org.infinispan.util.concurrent.CompletableFutures;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
public class PersistenceManagerStub implements PersistenceManager {
   @Start
   @Override
   public void start() {
   }

   @Stop
   @Override
   public void stop() {
   }

   @Override
   public boolean isEnabled() {
      return false;
   }

   @Override
   public boolean hasWriter() {
      return false;
   }

   @Override
   public boolean isPreloaded() {
      return false;
   }

   @Override
   public CompletionStage<Void> preload() {
      return CompletableFutures.completedNull();
   }

   @Override
   public void disableStore(String storeType) {
   }

   @Override
   public <T> Set<T> getStores(Class<T> storeClass) {
      return Collections.emptySet();
   }

   @Override
   public Collection<String> getStoresAsString() {
      return Collections.emptySet();
   }

   @Override
   public void purgeExpired() {
   }

   @Override
   public CompletionStage<Void> clearAllStores(Predicate<? super StoreConfiguration> predicate) {
      return CompletableFutures.completedNull();
   }

   @Override
   public boolean deleteFromAllStoresSync(Object key, int segment, Predicate<? super StoreConfiguration> predicate) {
      return false;
   }

   @Override
   public CompletionStage<Boolean> deleteFromAllStores(Object key, int segment, Predicate<? super StoreConfiguration> predicate) {
      return CompletableFutures.completedFalse();
   }

   @Override
   public <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue,
                                                                   boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate) {
      return Flowable.empty();
   }

   @Override
   public <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate) {
      return Flowable.empty();
   }

   @Override
   public <K> Publisher<K> publishKeys(Predicate<? super K> filter, Predicate<? super StoreConfiguration> predicate) {
      return Flowable.empty();
   }

   @Override
   public <K> Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter, Predicate<? super StoreConfiguration> predicate) {
      return Flowable.empty();
   }

   @Override
   public <K, V> CompletionStage<MarshallableEntry<K, V>> loadFromAllStores(Object key, boolean localInvocation, boolean includeStores) {
      return CompletableFutures.completedNull();
   }

   @Override
   public <K, V> CompletionStage<MarshallableEntry<K, V>> loadFromAllStores(Object key, int segment, boolean localInvocation, boolean includeStores) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> writeToAllNonTxStores(MarshallableEntry marshalledEntry, int segment, Predicate<? super StoreConfiguration> predicate, long flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Integer> size(Predicate<? super StoreConfiguration> predicate) {
      return CompletableFuture.completedFuture(0);
   }

   @Override
   public CompletionStage<Integer> size(IntSet segments) {
      return CompletableFuture.completedFuture(0);
   }

   @Override
   public void setClearOnStop(boolean clearOnStop) {
   }

   @Override
   public CompletionStage<Void> prepareAllTxStores(Transaction transaction, BatchModification batchModification, Predicate<? super StoreConfiguration> predicate) throws PersistenceException {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> commitAllTxStores(Transaction transaction, Predicate<? super StoreConfiguration> predicate) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> rollbackAllTxStores(Transaction transaction, Predicate<? super StoreConfiguration> predicate) {
      return CompletableFutures.completedNull();
   }

   @Override
   public void writeToAllNonTxStoresSync(MarshallableEntry marshalledEntry, int segment, Predicate<? super StoreConfiguration> predicate) {
   }

   @Override
   public CompletionStage<Void> writeBatchToAllNonTxStores(Iterable<MarshallableEntry> entries, Predicate<? super StoreConfiguration> predicate, long flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> deleteBatchFromAllNonTxStores(Iterable<Object> keys, Predicate<? super StoreConfiguration> predicate, long flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public boolean isAvailable() {
      return true;
   }

   @Override
   public boolean isReadOnly() {
      return false;
   }
}
