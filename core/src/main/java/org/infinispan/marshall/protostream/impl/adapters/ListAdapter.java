package org.infinispan.marshall.protostream.impl.adapters;

import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableList;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(
      value = List.class,
      subClassNames = {
            "java.util.Arrays$ArrayList",
            "java.util.ArrayList$SubList",
            "java.util.AbstractList$RandomAccessSubList",
            "java.util.Collections$EmptyList",
            "java.util.Collections$SingletonList",
            "java.util.Collections$SynchronizedRandomAccessList",
            "java.util.Collections$UnmodifiableRandomAccessList",
            "java.util.ImmutableCollections$ListN",
            "java.util.ImmutableCollections$List12"
      }
)
@ProtoTypeId(ProtoStreamTypeIds.ADAPTER_LIST)
public class ListAdapter {
   @ProtoFactory
   List<?> create(MarshallableList<?> elements) {
      return MarshallableList.unwrap(elements);
   }

   @ProtoField(1)
   MarshallableList<?> elements(List<?> list) {
      return MarshallableList.create(list);
   }
}
