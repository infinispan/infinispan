package org.infinispan.multimap.impl.function.list;

import java.util.Collection;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#subList(Object, long, long)}
 * to retrieve the sublist with indexes.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SUBLIST_FUNCTION)
public final class SubListFunction<K, V> implements ListBucketBaseFunction<K, V, Collection<V>> {

   @ProtoField(number = 1, defaultValue = "-1")
   final long from;

   @ProtoField(number = 2, defaultValue = "-1")
   final long to;

   @ProtoFactory
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
}
