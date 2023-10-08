package org.infinispan.jcache.embedded.functions;

import java.util.Objects;
import java.util.function.BiFunction;

import javax.cache.expiry.ExpiryPolicy;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.jcache.Expiration;
import org.infinispan.jcache.embedded.Durations;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.JCACHE_REPLACE_CONDITIONALLY)
public class ReplaceConditionally<K, V> implements BiFunction<V, EntryView.ReadWriteEntryView<K, V>, Boolean>, InjectableComponent {
   private final V oldValue;
   private ExpiryPolicy expiryPolicy;

   public ReplaceConditionally(V oldValue) {
      this.oldValue = oldValue;
   }

   @ProtoFactory
   ReplaceConditionally(MarshallableObject<V> oldValue) {
      this(MarshallableObject.unwrap(oldValue));
   }

   @ProtoField(1)
   MarshallableObject<V> getOldValue() {
      return MarshallableObject.create(oldValue);
   }

   @Override
   public void inject(ComponentRegistry registry) {
      expiryPolicy = registry.getComponent(ExpiryPolicy.class);
   }

   @Override
   public Boolean apply(V v, EntryView.ReadWriteEntryView<K, V> view) {
      if (view.find().isPresent()) {
         if (Objects.equals(oldValue, view.get())) {
            Durations.setWithTtl(view, v, expiryPolicy, Expiration.Operation.UPDATE);
            return true;
         } else {
            Durations.updateTtl(view, expiryPolicy);
         }
      }
      return false;
   }
}
