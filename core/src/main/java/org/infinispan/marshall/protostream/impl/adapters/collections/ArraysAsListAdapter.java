package org.infinispan.marshall.protostream.impl.adapters.collections;

import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(
      value = List.class,
      subClassNames = "java.util.Arrays$ArrayList"
)
@ProtoTypeId(ProtoStreamTypeIds.ADAPTER_COLLECTIONS_ARRAYS_AS_LIST)
public class ArraysAsListAdapter {
   @ProtoFactory
   List<?> create(MarshallableCollection<?> elements) {
      return elements == null ? null : Arrays.asList(elements.get().toArray());
   }

   @ProtoField(1)
   MarshallableCollection<?> elements(List<?> list) {
      return MarshallableCollection.create(list);
   }
}
