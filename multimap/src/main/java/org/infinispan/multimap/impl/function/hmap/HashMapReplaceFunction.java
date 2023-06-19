package org.infinispan.multimap.impl.function.hmap;

import static org.infinispan.multimap.impl.ExternalizerIds.HASH_MAP_REPLACE_FUNCTION;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.HashMapBucket;

public class HashMapReplaceFunction<K, HK, HV> extends HashMapBucketBaseFunction<K, HK, HV, Boolean> {

   public static final Externalizer EXTERNALIZER = new Externalizer();

   private final HK property;
   private final HV expected;
   private final HV replacement;

   public HashMapReplaceFunction(HK property, HV expected, HV replacement) {
      this.property = property;
      this.expected = expected;
      this.replacement = replacement;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<K, HashMapBucket<HK, HV>> view) {
      Optional<HashMapBucket<HK, HV>> existing = view.peek();
      HashMapBucket<HK, HV> bucket = existing.orElse(HashMapBucket.create(Map.of()));

      boolean replaced = bucket.replace(property, expected, replacement);
      if (replaced) view.set(bucket);

      return replaced;
   }

   @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
   private static class Externalizer implements AdvancedExternalizer<HashMapReplaceFunction> {

      @Override
      public Set<Class<? extends HashMapReplaceFunction>> getTypeClasses() {
         return Collections.singleton(HashMapReplaceFunction.class);
      }

      @Override
      public Integer getId() {
         return HASH_MAP_REPLACE_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, HashMapReplaceFunction object) throws IOException {
         output.writeObject(object.property);
         output.writeObject(object.expected);
         output.writeObject(object.replacement);
      }

      @Override
      public HashMapReplaceFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new HashMapReplaceFunction(input.readObject(), input.readObject(), input.readObject());
      }
   }
}
