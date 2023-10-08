package org.infinispan.multimap.impl.function.multimap;

import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.multimap.impl.Bucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by {@link org.infinispan.multimap.impl.EmbeddedMultimapCache#put(Object, Object)} to add a
 * key/value pair.
 *
 * @author Katia Aresti - karesti@redhat.com
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_PUT_FUNCTION)
public final class PutFunction<K, V> implements BaseFunction<K, V, Void> {

   private final V value;
   private final boolean supportsDuplicates;

   public PutFunction(V value, boolean supportsDuplicates) {
      this.value = value;
      this.supportsDuplicates = supportsDuplicates;
   }

   @ProtoFactory
   PutFunction(MarshallableObject<V> value, boolean supportsDuplicates) {
      this(MarshallableObject.unwrap(value), supportsDuplicates);
   }

   @ProtoField(1)
   MarshallableObject<V> getValue() {
      return MarshallableObject.create(value);
   }

   @ProtoField(2)
   boolean isSupportsDuplicates() {
      return supportsDuplicates;
   }

   @Override
   public Void apply(EntryView.ReadWriteEntryView<K, Bucket<V>> entryView) {
      Optional<Bucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         Bucket<V> newBucket = existing.get().add(value, supportsDuplicates);
         //don't change the cache is the value already exists. it avoids replicating a no-op
         if (newBucket != null) {
            entryView.set(newBucket);
         }
      } else {
         entryView.set(new Bucket<>(value));
      }

      return null;
   }
}
