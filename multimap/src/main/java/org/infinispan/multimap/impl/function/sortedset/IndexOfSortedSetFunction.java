package org.infinispan.multimap.impl.function.sortedset;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.SortedSetBucket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#indexOf(Object, Object, boolean)}
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class IndexOfSortedSetFunction<K, V> implements SortedSetBucketBaseFunction<K, V, SortedSetBucket.IndexValue> {
   public static final AdvancedExternalizer<IndexOfSortedSetFunction> EXTERNALIZER = new Externalizer();
   private final V member;
   private final boolean isRev;

   public IndexOfSortedSetFunction(V member, boolean isRev) {
      this.member = member;
      this.isRev = isRev;
   }

   @Override
   public SortedSetBucket.IndexValue apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         return existing.get().indexOf(member, isRev);
      }
      return null;
   }

   private static class Externalizer implements AdvancedExternalizer<IndexOfSortedSetFunction> {

      @Override
      public Set<Class<? extends IndexOfSortedSetFunction>> getTypeClasses() {
         return Collections.singleton(IndexOfSortedSetFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SORTED_SET_INDEX_OF_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, IndexOfSortedSetFunction object) throws IOException {
         output.writeObject(object.member);
         output.writeBoolean(object.isRev);
      }

      @Override
      public IndexOfSortedSetFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new IndexOfSortedSetFunction(input.readObject(), input.readBoolean());
      }
   }
}
