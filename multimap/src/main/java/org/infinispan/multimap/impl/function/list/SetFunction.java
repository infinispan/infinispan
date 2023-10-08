package org.infinispan.multimap.impl.function.list;

import java.util.Optional;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#set(Object, long, Object)}
 * to insert a key/value pair at the index of the multimap list value.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SET_FUNCTION)
public final class SetFunction<K, V> implements ListBucketBaseFunction<K, V, Boolean> {

   @ProtoField(value = 1, defaultValue = "-1")
   final long index;
   private final V value;

   public SetFunction(long index, V value) {
      this.index = index;
      this.value = value;
   }

   @ProtoFactory
   SetFunction(long index, MarshallableObject<V> value) {
      this(index, MarshallableObject.unwrap(value));
   }

   @ProtoField(2)
   MarshallableObject<V> getValue() {
      return MarshallableObject.create(value);
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         ListBucket<V> newBucket = existing.get().set(index, value);
         if (newBucket != null) {
            entryView.set(newBucket);
            return true;
         }

         throw new CacheException(new IndexOutOfBoundsException("Index is out of range"));
      }
      return false;
   }
}
