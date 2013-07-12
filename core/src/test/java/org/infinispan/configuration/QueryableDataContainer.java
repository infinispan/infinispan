package org.infinispan.configuration;

import org.infinispan.metadata.Metadata;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static java.util.Collections.synchronizedCollection;

public class QueryableDataContainer implements DataContainer {
	
	private static DataContainer delegate;
	
	public static void setDelegate(DataContainer delegate) {
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
	public Iterator<InternalCacheEntry> iterator() {
		loggedOperations.add("iterator()");
		return delegate.iterator();
	}

	@Override
	public InternalCacheEntry get(Object k) {
		loggedOperations.add("get(" + k + ")" );
		return delegate.get(k);
	}

	@Override
	public InternalCacheEntry peek(Object k) {
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
	public InternalCacheEntry remove(Object k) {
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
	public Set<InternalCacheEntry> entrySet() {
		loggedOperations.add("entrySet()" );
		return delegate.entrySet();
	}

	@Override
	public void purgeExpired() {
		loggedOperations.add("purgeExpired()" );
		delegate.purgeExpired();
	}
	
	public Collection<String> getLoggedOperations() {
	   return loggedOperations;
   }

}
