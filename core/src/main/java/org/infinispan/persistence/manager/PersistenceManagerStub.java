package org.infinispan.persistence.manager;

import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Executor;

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
   public void clearAllStores(AccessMode mode) {
   }

   @Override
   public boolean deleteFromAllStores(Object key, AccessMode mode) {
      return false;
   }

   @Override
   public void processOnAllStores(KeyFilter keyFilter, AdvancedCacheLoader.CacheLoaderTask task, boolean fetchValue, boolean fetchMetadata) {
   }

   @Override
   public void processOnAllStores(Executor executor, KeyFilter keyFilter, AdvancedCacheLoader.CacheLoaderTask task, boolean fetchValue, boolean fetchMetadata) {
   }

   @Override
   public void processOnAllStores(KeyFilter keyFilter, AdvancedCacheLoader.CacheLoaderTask task, boolean fetchValue, boolean fetchMetadata, AccessMode mode) {
   }

   @Override
   public void processOnAllStores(Executor executor, KeyFilter keyFilter, AdvancedCacheLoader.CacheLoaderTask task, boolean fetchValue, boolean fetchMetadata, AccessMode mode) {
   }

   @Override
   public MarshalledEntry loadFromAllStores(Object key, InvocationContext context) {
      return null;
   }

   @Override
   public void writeToAllStores(MarshalledEntry marshalledEntry, AccessMode modes) {
   }

   @Override
   public void writeToAllStores(MarshalledEntry marshalledEntry, AccessMode modes, Set<Flag> flags) {
   }

   @Override
   public AdvancedCacheLoader getStateTransferProvider() {
      return null;
   }

   @Override
   public int size() {
      return 0;
   }

   @Override
   public void setClearOnStop(boolean clearOnStop) {
   }
}
