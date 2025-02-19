package org.infinispan.multimap.impl.function.multimap;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.multimap.impl.Bucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by {@link org.infinispan.multimap.impl.EmbeddedMultimapCache#containsKey(Object)} and
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapCache#containsEntry(Object, Object)}.
 *
 * @author Katia Aresti - karesti@redhat.com
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_CONTAINS_FUNCTION)
public final class ContainsFunction<K, V> implements BaseFunction<K, V, Boolean> {

   private final V value;

   public ContainsFunction() {
      this.value = null;
   }

   @ProtoFactory
   ContainsFunction(MarshallableObject<V> value) {
      this.value = MarshallableObject.unwrap(value);
   }

   @ProtoField(1)
   public MarshallableObject<V> getValue() {
      return MarshallableObject.create(value);
   }

   /**
    * Call this constructor to create a function that checks if a key/value pair exists
    * <ul>
    * <li>if the key is null, the value will be searched in any key
    * <li>if the value is null, the key will be searched
    * <li>key-value pair are not null, the entry will be searched
    * <li>key-value pair are null, a {@link NullPointerException} will be raised
    * </ul>
    *
    * @param value value to be checked on the key
    */
   public ContainsFunction(V value) {
      this.value = value;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<K, Bucket<V>> entryView) {
      return value == null ?
            entryView.find().isPresent() :
            entryView.find().map(values -> values.contains(value)).orElse(Boolean.FALSE);
   }
}
