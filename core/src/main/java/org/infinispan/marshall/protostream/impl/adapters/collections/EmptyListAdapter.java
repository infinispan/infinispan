package org.infinispan.marshall.protostream.impl.adapters.collections;

import java.util.Collections;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(
      value = List.class,
      subClassNames = "java.util.Collections$EmptyList"
)
@ProtoTypeId(ProtoStreamTypeIds.ADAPTER_COLLECTIONS_EMPTY_LIST)
public class EmptyListAdapter {
   @ProtoFactory
   List<?> create() {
      return Collections.emptyList();
   }
}
