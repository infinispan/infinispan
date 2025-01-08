package org.infinispan.marshall.protostream.impl.adapters.collections;

import java.util.Collections;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(
      value = List.class,
      subClassNames = "java.util.Collections$SingletonList"
)
@ProtoTypeId(ProtoStreamTypeIds.ADAPTER_COLLECTIONS_SINGLETON_LIST)
public class SingletonListAdapter {
   @ProtoFactory
   List<?> create(MarshallableObject<?> element) {
      return Collections.singletonList(MarshallableObject.unwrap(element));
   }

   @ProtoField(1)
   MarshallableObject<?> getElement(List<?> list) {
      return MarshallableObject.create(list.get(0));
   }
}
