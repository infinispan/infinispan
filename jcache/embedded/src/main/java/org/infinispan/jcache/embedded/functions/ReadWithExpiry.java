package org.infinispan.jcache.embedded.functions;

import java.util.function.Function;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.MetaParam;
import org.infinispan.jcache.Expiration;
import org.infinispan.jcache.embedded.Durations;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.JCACHE_READ_WITH_EXPIRY)
public class ReadWithExpiry<K, V> implements Function<EntryView.ReadWriteEntryView<K, V>, V>, InjectableComponent {
   private ExpiryPolicy expiryPolicy;

   @Override
   public void inject(ComponentRegistry registry) {
      expiryPolicy = registry.getComponent(ExpiryPolicy.class);
   }

   @Override
   public V apply(EntryView.ReadWriteEntryView<K, V> view) {
      if (view.find().isPresent()) {
         V value = view.get();
         Duration ttl = Expiration.getExpiry(expiryPolicy, Expiration.Operation.ACCESS);
         if (ttl == null) {
            // noop
         } else if (ttl.isZero()) {
            view.remove();
         } else {
            view.set(value, new MetaParam.MetaLifespan(Durations.toMs(ttl)));
         }
         return value;
      } else {
         return null;
      }
   }
}
