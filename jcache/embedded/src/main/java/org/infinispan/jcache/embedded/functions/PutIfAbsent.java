package org.infinispan.jcache.embedded.functions;

import java.util.function.BiFunction;

import javax.cache.expiry.ExpiryPolicy;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.jcache.Expiration;
import org.infinispan.jcache.embedded.Durations;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.JCACHE_PUT_IF_ABSENT)
public class PutIfAbsent<K, V> implements BiFunction<V, EntryView.ReadWriteEntryView<K, V>, Boolean>, InjectableComponent {
   private ExpiryPolicy expiryPolicy;

   @Override
   public void inject(ComponentRegistry registry) {
      expiryPolicy = registry.getComponent(ExpiryPolicy.class);
   }

   @Override
   public Boolean apply(V v, EntryView.ReadWriteEntryView<K, V> view) {
      if (view.find().isPresent()) {
         return false;
      }
      Durations.setWithTtl(view, v, expiryPolicy, Expiration.Operation.CREATION);
      return true;
   }
}
