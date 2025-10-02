package org.infinispan.marshall.protostream.impl.adapters;

import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(
      value = Map.class,
      subClassNames = {
            "java.util.HashMap",
            "java.util.Collections$EmptyMap",
            "java.util.Collections$SingletonMap",
            "java.util.ImmutableCollections$Map1",
            "java.util.ImmutableCollections$MapN"
      }
)
@ProtoTypeId(ProtoStreamTypeIds.ADAPTER_MAP)
public class MapAdapter {
   @ProtoFactory
   Map<?, ?> create(MarshallableMap<?, ?> elements) {
      return MarshallableMap.unwrap(elements);
   }

   @ProtoField(1)
   MarshallableMap<?, ?> elements(Map<?, ?> map) {
      return MarshallableMap.create(map);
   }
}
