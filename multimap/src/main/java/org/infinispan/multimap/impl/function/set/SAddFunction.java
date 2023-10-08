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
 * {@link org.infinispan.multimap.impl.EmbeddedSetCache#add}
 * to add elements to a Set.
 *
 * @author Vittorio Rigamonti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of
 *      Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_S_ADD_FUNCTION)
public final class SAddFunction<K, V> implements SetBucketBaseFunction<K, V, Long> {

   private final Collection<V> values;

   public SAddFunction(Collection<V> values) {
      this.values = values;
   }

   @ProtoFactory
   SAddFunction(MarshallableCollection<V> values) {
      this.values = MarshallableCollection.unwrap(values);
   }

   @ProtoField(1)
   MarshallableCollection<V> getValues() {
      return MarshallableCollection.create(values);
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, SetBucket<V>> entryView) {
      Optional<SetBucket<V>> existing = entryView.peek();
      long added = 0L;
      var s = existing.orElseGet(SetBucket::new);
      var initSize = s.size();
      var res = s.addAll(values);
      s = res.bucket();
      if (res.result()) {
         added = s.size() - initSize;
      }
      // don't change the cache if the value already exists. it avoids replicating a
      // no-op
      if (added > 0) {
         entryView.set(s);
      }
      return added;
   }
}
