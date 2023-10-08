package org.infinispan.multimap.impl.function.multimap;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.Bucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by {@link org.infinispan.multimap.impl.EmbeddedMultimapCache#get(Object)}
 * to get a key's value.
 *
 * @author Katia Aresti - karesti@redhat.com
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_GET_FUNCTION)
public final class GetFunction<K, V> implements BaseFunction<K, V, Collection<V>> {

   @ProtoField(1)
   final boolean supportsDuplicates;

   @ProtoFactory
   public GetFunction(boolean supportsDuplicates) {
      this.supportsDuplicates = supportsDuplicates;
   }

   @Override
   public Collection<V> apply(EntryView.ReadWriteEntryView<K, Bucket<V>> entryView) {
      Optional<Bucket<V>> valuesOpt = entryView.find();
      if (valuesOpt.isPresent()) {
         return supportsDuplicates ? entryView.find().get().toList() : entryView.find().get().toSet();
      } else {
         return Collections.emptySet();
      }
   }
}
