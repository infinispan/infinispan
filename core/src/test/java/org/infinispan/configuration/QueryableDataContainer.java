package org.infinispan.configuration;

import static java.util.Collections.synchronizedCollection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;

@Scope(Scopes.NAMED_CACHE)
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
   public Iterator<InternalCacheEntry<Object, Object>> iteratorIncludingExpired() {
      loggedOperations.add("expiredIterator()");
      return delegate.iteratorIncludingExpired();
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
   public int sizeIncludingExpired() {
      loggedOperations.add("sizeIncludingExpired()" );
      return delegate.sizeIncludingExpired();
   }

   @Stop
   @Override
   public void clear() {
      loggedOperations.add("clear()" );
      delegate.clear();
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
}
