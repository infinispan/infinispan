package org.infinispan.jcache.embedded.functions;

import java.util.Optional;
import java.util.function.BiFunction;

import javax.cache.expiry.ExpiryPolicy;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.jcache.Expiration;
import org.infinispan.jcache.embedded.Durations;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.JCACHE_GET_AND_PUT)
public class GetAndPut<K, V> implements BiFunction<V, EntryView.ReadWriteEntryView<K, V>, V>, InjectableComponent {
   protected ExpiryPolicy expiryPolicy;

   @Override
   public void inject(ComponentRegistry registry) {
      expiryPolicy = registry.getComponent(ExpiryPolicy.class);
   }

   @Override
   public V apply(V v, EntryView.ReadWriteEntryView<K, V> view) {
      Optional<V> prev = view.find();
      Durations.setWithTtl(view, v, expiryPolicy, prev.isPresent() ?
            Expiration.Operation.UPDATE : Expiration.Operation.CREATION);
      return prev.orElse(null);
   }
}
