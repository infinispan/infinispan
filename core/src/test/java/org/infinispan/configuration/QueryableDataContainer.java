package org.infinispan.configuration;

import org.infinispan.filter.KeyFilter;
import org.infinispan.metadata.Metadata;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.commons.util.concurrent.ParallelIterableMap.KeyValueAction;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.jboss.util.NotImplementedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static java.util.Collections.synchronizedCollection;

public class QueryableDataContainer implements DataContainer<Object, Object> {

   // Since this static field is here, we can't use generic types properly
   private static DataContainer<Object, Object> delegate;

   public static void setDelegate(DataContainer<Object, Object> delegate) {
      QueryableDataContainer.delegate = delegate;
   }

   private final Collection<String> loggedOperations;

   public void setFoo(String foo) {
      loggedOperations.add("setFoo(" + foo + ")");
   }

   public QueryableDataContainer() {
      this.loggedOperations = synchronizedCollection(new ArrayList<String>());
   }

   @Override
   public Iterator<InternalCacheEntry<Object, Object>> iterator() {
      loggedOperations.add("iterator()");
      return delegate.iterator();
   }

   @Override
   public InternalCacheEntry<Object, Object> get(Object k) {
      loggedOperations.add("get(" + k + ")" );
      return delegate.get(k);
   }

   @Override
   public InternalCacheEntry<Object, Object> peek(Object k) {
      loggedOperations.add("peek(" + k + ")" );
      return delegate.peek(k);
   }

   @Override
   public void put(Object k, Object v, Metadata metadata) {
      loggedOperations.add("put(" + k + ", " + v + ", " + metadata + ")");
      delegate.put(k, v, metadata);
   }

   @Override
   public boolean containsKey(Object k) {
      loggedOperations.add("containsKey(" + k + ")" );
      return delegate.containsKey(k);
   }

   @Override
   public InternalCacheEntry<Object, Object> remove(Object k) {
      loggedOperations.add("remove(" + k + ")" );
      return delegate.remove(k);
   }

   @Override
   public int size() {
      loggedOperations.add("size()" );
      return delegate.size();
   }

   @Override
   public void clear() {
      loggedOperations.add("clear()" );
      delegate.clear();
   }

   @Override
   public Set<Object> keySet() {
      loggedOperations.add("keySet()" );
      return delegate.keySet();
   }

   @Override
   public Collection<Object> values() {
      loggedOperations.add("values()" );
      return delegate.values();
   }

   @Override
   public Set<InternalCacheEntry<Object, Object>> entrySet() {
      loggedOperations.add("entrySet()" );
      return delegate.entrySet();
   }

   @Override
   public void purgeExpired() {
      loggedOperations.add("purgeExpired()" );
      delegate.purgeExpired();
   }

   @Override
   public void evict(Object key) {
      loggedOperations.add("evict(" + key + ")");
      delegate.evict(key);
   }

   @Override
   public InternalCacheEntry<Object, Object> compute(Object key, ComputeAction<Object, Object> action) {
      loggedOperations.add("compute(" + key + "," + action + ")");
      return delegate.compute(key, action);
   }

   public Collection<String> getLoggedOperations() {
      return loggedOperations;
   }

   @Override
   public void executeTask(final KeyFilter<? super Object> filter, KeyValueAction <? super Object, InternalCacheEntry<Object, Object>> action)
         throws InterruptedException {
      throw new NotImplementedException();
   }

   @Override
   public void executeTask(final KeyValueFilter<? super Object, ? super Object> filter, KeyValueAction<? super Object, InternalCacheEntry<Object, Object>> action) throws InterruptedException {
      throw new NotImplementedException();
   }
}
