package org.infinispan.multimap.impl.function.list;

import static org.infinispan.commons.marshall.MarshallUtil.unmarshallCollection;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.ListBucket;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#replace(Object, List)}
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public final class ReplaceListFunction<K, V> implements ListBucketBaseFunction<K, V, Long> {
   public static final AdvancedExternalizer<ReplaceListFunction> EXTERNALIZER = new Externalizer();
   private final List<V> values;

   public ReplaceListFunction(List<V> values) {
      this.values = values;
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      ListBucket bucket = null;
      if (existing.isPresent()) {
        bucket = existing.get();
      } else if (values != null && !values.isEmpty()){
         bucket = new ListBucket();
      }

      if (bucket != null) {
         bucket.replace(values);
         if (bucket.isEmpty()) {
            entryView.remove();
         } else {
            entryView.set(bucket);
         }
         return bucket.size();
      }

      // nothing has been done
      return 0L;
   }

   private static class Externalizer implements AdvancedExternalizer<ReplaceListFunction> {

      @Override
      public Set<Class<? extends ReplaceListFunction>> getTypeClasses() {
         return Collections.singleton(ReplaceListFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.REPLACE_LIST_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, ReplaceListFunction object) throws IOException {
         MarshallUtil.marshallCollection(object.values, output);
      }

      @Override
      public ReplaceListFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         List values = unmarshallCollection(input, ArrayList::new);
         return new ReplaceListFunction(values);
      }
   }
}
