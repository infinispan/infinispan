package org.infinispan.marshall.protostream.impl.adapters.collections;

import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(
      value = Set.class,
      subClassNames = "java.util.Collections$EmptySet"
)
@ProtoTypeId(ProtoStreamTypeIds.ADAPTER_COLLECTIONS_EMPTY_SET)
public class EmptySetAdapter {
   @ProtoFactory
   Set<?> create() {
      return Collections.emptySet();
   }
}
