package org.infinispan.multimap.impl.function.hmap;

import static org.infinispan.multimap.impl.ExternalizerIds.HASH_MAP_VALUES_FUNCTION;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.HashMapBucket;

/**
 * Serializable function used by {@link org.infinispan.multimap.impl.EmbeddedMultimapPairCache#values(Object)}.
 * <p>
 *    This function returns the values of the {@link HashMapBucket}, or an empty set if the entry is not found.
 * </p>
 *
 * @param <K>: Key type to identify the {@link HashMapBucket}.
 * @param <HK>: The {@link HashMapBucket} key type.
 * @param <HV>: The {@link HashMapBucket} value type.
 * @since 15.0
 * @see BaseFunction
 */
public class HashMapValuesFunction<K, HK, HV> extends HashMapBucketBaseFunction<K, HK, HV, Collection<HV>> {

   public static final Externalizer EXTERNALIZER = new Externalizer();

   @Override
   public Collection<HV> apply(EntryView.ReadWriteEntryView<K, HashMapBucket<HK, HV>> view) {
      Optional<HashMapBucket<HK, HV>> existing = view.peek();
      if (existing.isPresent()) {
         return existing.get().values();
      }
      return Collections.emptyList();
   }

   @SuppressWarnings({"rawtypes", "deprecation"})
   private static class Externalizer implements AdvancedExternalizer<HashMapValuesFunction> {

      @Override
      public Set<Class<? extends HashMapValuesFunction>> getTypeClasses() {
         return Collections.singleton(HashMapValuesFunction.class);
      }

      @Override
      public Integer getId() {
         return HASH_MAP_VALUES_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, HashMapValuesFunction object) throws IOException { }

      @Override
      public HashMapValuesFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new HashMapValuesFunction();
      }
   }
}
