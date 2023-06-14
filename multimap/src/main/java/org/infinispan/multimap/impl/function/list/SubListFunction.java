package org.infinispan.multimap.impl.function.list;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.ListBucket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#subList(Object, long, long)}
 * to retrieve the sublist with indexes.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class SubListFunction<K, V> implements ListBucketBaseFunction<K, V, Collection<V>> {
   public static final AdvancedExternalizer<SubListFunction> EXTERNALIZER = new SubListFunction.Externalizer();
   private final long from;
   private final long to;

   public SubListFunction(long from, long to) {
      this.from = from;
      this.to = to;
   }

   @Override
   public Collection<V> apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         return existing.get().sublist(from, to);
      }
      return null;
   }

   private static class Externalizer implements AdvancedExternalizer<SubListFunction> {

      @Override
      public Set<Class<? extends SubListFunction>> getTypeClasses() {
         return Collections.singleton(SubListFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SUBLIST_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, SubListFunction object) throws IOException {
         output.writeLong(object.from);
         output.writeLong(object.to);
      }

      @Override
      public SubListFunction readObject(ObjectInput input) throws IOException {
         return new SubListFunction(input.readLong(), input.readLong());
      }
   }
}
