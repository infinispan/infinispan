package org.infinispan.jcache.embedded.functions;

import org.infinispan.functional.EntryView;
import org.infinispan.jcache.Expiration;
import org.infinispan.jcache.embedded.Durations;

public class Put<K, V> extends GetAndPut<K, V> {
   @Override
   public V apply(V v, EntryView.ReadWriteEntryView<K, V> view) {
      Durations.setWithTtl(view, v, expiryPolicy, view.peek().isPresent() ?
            Expiration.Operation.UPDATE : Expiration.Operation.CREATION);
      return null;
   }
}
