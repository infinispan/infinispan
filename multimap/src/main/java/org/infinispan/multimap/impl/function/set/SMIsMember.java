package org.infinispan.multimap.impl.function.set;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
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
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_S_M_IS_MEMBER_FUNCTION)
public final class SMIsMember<K, V> implements SetBucketBaseFunction<K, V, List<Long>> {

   private final V[] values;

   public SMIsMember(V... values) {
      this.values = values;
   }

   @ProtoFactory
   SMIsMember(MarshallableArray<V> values) {
      this.values = MarshallableArray.unwrap(values);
   }

   @ProtoField(1)
   MarshallableArray<V> getValues() {
      return MarshallableArray.create(values);
   }

   @Override
   public List<Long> apply(EntryView.ReadWriteEntryView<K, SetBucket<V>> entryView) {
      var result = new ArrayList<Long>();
      Optional<SetBucket<V>> existing = entryView.peek();
      var s = existing.isPresent() ? existing.get() : new SetBucket<V>();
      for (var v : values) {
         result.add(s.contains(v) ? 1L : 0L);
      }
      return result;
   }
}
