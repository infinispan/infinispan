package org.infinispan.jcache.embedded.functions;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import javax.cache.expiry.ExpiryPolicy;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.jcache.Expiration;
import org.infinispan.jcache.embedded.Durations;
import org.infinispan.jcache.embedded.ExternalizerIds;

public class ReplaceConditionally<K, V> implements BiFunction<V, EntryView.ReadWriteEntryView<K, V>, Boolean>, InjectableComponent {
   private final V oldValue;
   private ExpiryPolicy expiryPolicy;

   public ReplaceConditionally(V oldValue) {
      this.oldValue = oldValue;
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

   public static class Externalizer implements AdvancedExternalizer<ReplaceConditionally> {
      @Override
      public Set<Class<? extends ReplaceConditionally>> getTypeClasses() {
         return Util.asSet(ReplaceConditionally.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.REPLACE_CONDITIONALLY;
      }

      @Override
      public void writeObject(ObjectOutput output, ReplaceConditionally object) throws IOException {
         output.writeObject(object.oldValue);
      }

      @Override
      public ReplaceConditionally readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ReplaceConditionally(input.readObject());
      }
   }
}
