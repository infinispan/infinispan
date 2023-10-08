package org.infinispan.marshall.protostream.impl.adapters.collections;

import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(
      value = List.class,
      subClassNames = {
            "java.util.ImmutableCollections$ListN",
            "java.util.ImmutableCollections$List12"
      }
)
@ProtoTypeId(ProtoStreamTypeIds.ADAPTER_COLLECTIONS_LIST_OF)
public class ListOfAdapter {
   @ProtoFactory
   List<?> create(MarshallableCollection<?> elements) {
      return elements == null ? null : List.of(elements.get().toArray());
   }

   @ProtoField(1)
   MarshallableCollection<?> elements(List<?> list) {
      return MarshallableCollection.create(list);
   }
}
