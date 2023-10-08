package org.infinispan.multimap.impl.function.set;

import java.util.Collection;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

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
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_S_REMOVE_FUNCTION)
public final class SRemoveFunction<K, V> implements SetBucketBaseFunction<K, V, Long> {

   private final Collection<V> values;

   public SRemoveFunction(Collection<V> values) {
      this.values = values;
   }

   @ProtoFactory
   SRemoveFunction(MarshallableCollection<V> values) {
      this.values = MarshallableCollection.unwrap(values);
   }

   @ProtoField(1)
   MarshallableCollection<V> getValues() {
      return MarshallableCollection.create(values);
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, SetBucket<V>> entryView) {
      Optional<SetBucket<V>> existing = entryView.peek();
      long removed = 0L;
      if (existing.isEmpty()) {
         return 0L;
      }
      SetBucket<V> s = existing.get();
      int initSize = s.size();
      var res = s.removeAll(values);
      s = res.bucket();
      if (res.result()) {
         removed = initSize - s.size();
      }
      // don't change the cache if the value already exists. it avoids replicating a
      // no-op
      if (removed > 0) {
         if (!s.isEmpty()) {
            entryView.set(s);
         } else {
            entryView.remove();
         }
      }
      return removed;
   }
}
