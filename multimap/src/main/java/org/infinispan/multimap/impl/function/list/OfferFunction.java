package org.infinispan.multimap.impl.function.list;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#offerFirst(Object, Object)}
 * and
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#offerLast(Object, Object)}
 * (Object, Object)}
 * to insert a key/value pair at the head or the tail of the multimap list
 * value.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of
 *      Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_OFFER_FUNCTION)
public final class OfferFunction<K, V> implements ListBucketBaseFunction<K, V, Void> {

   private final Collection<V> value;
   private final boolean first;

   public OfferFunction(V value, boolean first) {
      this.value = Arrays.asList(value);
      this.first = first;
   }

   public OfferFunction(Collection<V> value, boolean first) {
      this.value = value;
      this.first = first;
   }

   @ProtoFactory
   OfferFunction(MarshallableCollection<V> value, boolean first) {
      this(MarshallableCollection.unwrap(value), first);
   }

   @ProtoField(1)
   MarshallableCollection<V> getValue() {
      return MarshallableCollection.create(value);
   }

   @ProtoField(value = 2, defaultValue = "false")
   boolean isFirst() {
      return first;
   }

   @Override
   public Void apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         ListBucket<V> newBucket = existing.get().offer(value, first);
         // don't change the cache is the value already exists. it avoids replicating a
         // no-op
         if (newBucket != null) {
            entryView.set(newBucket);
         }
      } else {
         if (first) {
            // in this case collection needs to be reversed (if it supports order)
            var copy = new ArrayList<>(value);
            Collections.reverse(copy);
            entryView.set(ListBucket.create(copy));
         } else {
            entryView.set(ListBucket.create(value));
         }
      }

      return null;
   }
}
