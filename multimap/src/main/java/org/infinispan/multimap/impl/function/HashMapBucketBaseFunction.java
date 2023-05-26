package org.infinispan.multimap.impl.function;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.internal.MultimapDataConverter;
import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.util.function.SerializableFunction;

public abstract class HashMapBucketBaseFunction<K, HK, HV, R>
      implements SerializableFunction<EntryView.ReadWriteEntryView<K, HashMapBucket<HK, HV>>, R>, InjectableComponent {

   protected final MultimapDataConverter<HK, HV> converter;

   public HashMapBucketBaseFunction(MultimapDataConverter<HK, HV> converter) {
      this.converter = converter;
   }

   @Override
   public void inject(ComponentRegistry registry) {
      converter.inject(registry);
   }

   protected Object convertKey(HK key) {
      return converter.convertKeyToStore(key);
   }

   protected Object convertValue(HV value) {
      return converter.convertValueToStore(value);
   }
}
