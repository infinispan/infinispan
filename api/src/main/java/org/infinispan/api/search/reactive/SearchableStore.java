package org.infinispan.api.search.reactive;

public interface SearchableStore<V> {
   ReactiveQuery find(String ickleQuery);

   ReactiveContinuousQuery findContinuously(String ickleQuery);
}
