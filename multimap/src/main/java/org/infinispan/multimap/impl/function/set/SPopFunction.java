package org.infinispan.multimap.impl.function.set;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SetBucket;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedSetCache#remove}
 * to remove elements to a Set.
 *
 * @author Vittorio Rigamonti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of
 *      Functions</a>
 * @since 15.0
 */
public final class SPopFunction<K, V> implements SetBucketBaseFunction<K, V, Collection<V>> {
   public static final AdvancedExternalizer<SPopFunction> EXTERNALIZER = new Externalizer();
   private final long count;
   private final boolean remove;

   public SPopFunction(long count, boolean remove) {
      this.count = count;
      this.remove = remove;
   }

   @Override
   public Collection<V> apply(EntryView.ReadWriteEntryView<K, SetBucket<V>> entryView) {
      Optional<SetBucket<V>> existing = entryView.peek();
      if (count == 0 || !existing.isPresent()) {
         return Collections.emptyList();
      }
      var s = existing.get();
      if (count > 0) {
         var popped = getRandomSubset(s.toList(), count);
         if (remove) {
            s.removeAll(popped);
         }
         return popped;
      }
      var list = s.toList();
      return ThreadLocalRandom.current().ints(-count, 0, s.size()).mapToObj(i -> list.get(i)).collect(Collectors.toList());
   }

   public static <T> Collection<T> getRandomSubset(List<T> list, long count) {
      if (list.size() <= count) {
         return list;
      } else {
         Collections.shuffle(list);
         return list.stream().limit(count).collect(Collectors.toList());
      }
   }

   private static class Externalizer implements AdvancedExternalizer<SPopFunction> {

      @Override
      public Set<Class<? extends SPopFunction>> getTypeClasses() {
         return Collections.singleton(SPopFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SET_POP_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, SPopFunction object) throws IOException {
         output.writeLong(object.count);
         output.writeBoolean(object.remove);
      }

      @Override
      public SPopFunction<?, ?> readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         var count = input.readLong();
         var remove = input.readBoolean();
         return new SPopFunction<>(count, remove);
      }
   }
}
