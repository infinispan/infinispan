package org.infinispan.jcache.embedded.functions;

import java.util.function.BiFunction;

import javax.cache.expiry.ExpiryPolicy;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.jcache.Expiration;
import org.infinispan.jcache.embedded.Durations;

public class GetAndReplace<K, V> implements BiFunction<V, EntryView.ReadWriteEntryView<K, V>, V>, InjectableComponent {
   private ExpiryPolicy expiryPolicy;

   @Override
   public void inject(ComponentRegistry registry) {
      expiryPolicy = registry.getComponent(ExpiryPolicy.class);
   }

   @Override
   public V apply(V v, EntryView.ReadWriteEntryView<K, V> view) {
      if (view.find().isPresent()) {
         V prev = view.get();
         Durations.setWithTtl(view, v, expiryPolicy, Expiration.Operation.UPDATE);
         return prev;
      } else {
         return null;
      }
   }
}
