package org.infinispan.persistence.manager;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

import javax.transaction.Transaction;

import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.BatchModification;
import org.reactivestreams.Publisher;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@SurvivesRestarts
public class PersistenceManagerStub implements PersistenceManager {
   @Override
   public void start() {
   }

   @Override
   public void stop() {
   }

   @Override
   public boolean isEnabled() {
      return false;
   }

   @Override
   public boolean isPreloaded() {
      return false;
   }

   @Override
   public void preload() {
   }

   @Override
   public void disableStore(String storeType) {
   }

   @Override
   public <T> Set<T> getStores(Class<T> storeClass) {
      return Collections.EMPTY_SET;
   }

   @Override
   public Collection<String> getStoresAsString() {
      return Collections.EMPTY_SET;
   }

   @Override
   public void purgeExpired() {
   }

   @Override
   public void clearAllStores(Predicate<? super StoreConfiguration> predicate) {
   }

   @Override
   public boolean deleteFromAllStores(Object key, int segment, Predicate<? super StoreConfiguration> predicate) {
      return false;
   }

   @Override
   public <K, V> Publisher<MarshalledEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue,
         boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate) {
      return null;
   }

   @Override
   public <K, V> Publisher<MarshalledEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate) {
      return null;
   }

   @Override
   public <K> Publisher<K> publishKeys(Predicate<? super K> filter, Predicate<? super StoreConfiguration> predicate) {
      return null;
   }

   @Override
   public <K> Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter, Predicate<? super StoreConfiguration> predicate) {
      return null;
   }

   @Override
   public MarshalledEntry loadFromAllStores(Object key, boolean localInvocation, boolean includeStores) {
      return null;
   }

   @Override
   public MarshalledEntry loadFromAllStores(Object key, int segment, boolean localInvocation, boolean includeStores) {
      return null;
   }

   @Override
   public void writeToAllNonTxStores(MarshalledEntry marshalledEntry, int segment, Predicate<? super StoreConfiguration> predicates) {
   }

   @Override
   public void writeToAllNonTxStores(MarshalledEntry marshalledEntry, int segment, Predicate<? super StoreConfiguration> predicates, long flags) {
   }

   @Override
   public AdvancedCacheLoader getStateTransferProvider() {
      return null;
   }

   @Override
   public int size(Predicate<? super StoreConfiguration> predicate) {
      return 0;
   }

   @Override
   public int size(IntSet segments) {
      return 0;
   }

   @Override
   public void setClearOnStop(boolean clearOnStop) {
   }

   @Override
   public void prepareAllTxStores(Transaction transaction, BatchModification batchModification, Predicate<? super StoreConfiguration> predicate) throws PersistenceException {
   }

   @Override
   public void commitAllTxStores(Transaction transaction, Predicate<? super StoreConfiguration> predicate) {
   }

   @Override
   public void rollbackAllTxStores(Transaction transaction, Predicate<? super StoreConfiguration> predicate) {
   }

   @Override
   public void writeBatchToAllNonTxStores(Iterable<MarshalledEntry> entries, Predicate<? super StoreConfiguration> predicate, long flags) {
   }

   @Override
   public void deleteBatchFromAllNonTxStores(Iterable<Object> keys, Predicate<? super StoreConfiguration> predicate, long flags) {
   }

   @Override
   public boolean isAvailable() {
      return true;
   }
}
