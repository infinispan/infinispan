package org.infinispan.multimap.impl.function.multimap;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.multimap.impl.Bucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by {@link org.infinispan.multimap.impl.EmbeddedMultimapCache#remove(Object)} and {@link
 * org.infinispan.multimap.impl.EmbeddedMultimapCache#remove(Object, Object)} to remove a key or a key/value pair from
 * the Multimap Cache, if such exists.
 * <p>
 * {@link #apply(EntryView.ReadWriteEntryView)} will return {@link Boolean#TRUE} when the operation removed a key or a
 * key/value pair and will return {@link Boolean#FALSE} if the key or key/value pair does not exist
 *
 * @author Katia Aresti - karesti@redhat.com
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_REMOVE_FUNCTION)
public final class RemoveFunction<K, V> implements BaseFunction<K, V, Boolean> {

   private final V value;
   private final boolean supportsDuplicates;

   /**
    * Call this constructor to create a function that removed a key
    */
   public RemoveFunction() {
      this.value = null;
      this.supportsDuplicates = false;
   }

   /**
    * Call this constructor to create a function that removed a key/value pair
    *
    * @param value value to be removed
    */
   public RemoveFunction(V value, boolean supportsDuplicates) {
      this.value = value;
      this.supportsDuplicates = supportsDuplicates;
   }

   @ProtoFactory
   RemoveFunction(MarshallableObject<V> value, boolean supportsDuplicates) {
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
   public Boolean apply(EntryView.ReadWriteEntryView<K, Bucket<V>> entryView) {
      Boolean removed;
      if (value == null) {
         removed = removeKey(entryView);
      } else {
         removed = removeKeyValue(entryView);
      }
      return removed;
   }

   private Boolean removeKeyValue(EntryView.ReadWriteEntryView<K, Bucket<V>> entryView) {
      return entryView.find().map(bucket -> {
               Bucket<V> newBucket = bucket.remove(value, supportsDuplicates);
               if (newBucket != null) {
                  if (newBucket.isEmpty()) {
                     entryView.remove();
                  } else {
                     entryView.set(newBucket);
                  }
                  return Boolean.TRUE;
               }
               return Boolean.FALSE;
            }
      ).orElse(Boolean.FALSE);
   }

   private Boolean removeKey(EntryView.ReadWriteEntryView<K, Bucket<V>> entryView) {
      return entryView.find().map(values -> {
         entryView.remove();
         return Boolean.TRUE;
      }).orElse(Boolean.FALSE);
   }
}
