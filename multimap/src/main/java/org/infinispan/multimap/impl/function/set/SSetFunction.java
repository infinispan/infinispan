package org.infinispan.multimap.impl.function.set;

import java.util.Collection;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedSetCache#set}
 * to operator on Sets.
 *
 * @author Vittorio Rigamonti
 * @see <a href="https://infinispan.org/documentation/">Marshalling of
 *      Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_S_SET_FUNCTION)
public final class SSetFunction<K, V> implements SetBucketBaseFunction<K, V, Long> {

   private final Collection<V> values;

   public SSetFunction(Collection<V> values) {
      this.values = values;
   }

   @ProtoFactory
   SSetFunction(MarshallableCollection<V> values) {
      this.values = MarshallableCollection.unwrap(values);
   }

   @ProtoField(1)
   MarshallableCollection<V> getValues() {
      return MarshallableCollection.create(values);
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, SetBucket<V>> entryView) {
      if (values.isEmpty()) {
         if (entryView.peek().isPresent()) {
            entryView.remove();
         }
         return 0L;
      }
      var set = SetBucket.create(values);
      entryView.set(set);
      return (long) set.size();
   }
}
