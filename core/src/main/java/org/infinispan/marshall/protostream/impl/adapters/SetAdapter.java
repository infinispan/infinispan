package org.infinispan.marshall.protostream.impl.adapters;

import java.util.Set;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableSet;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(
      value = Set.class,
      subClassNames = {
            "java.util.Collections$EmptySet",
            "java.util.Collections$SingletonSet",
            "java.util.Collections$SynchronizedSet",
            "java.util.Collections$UnmodifiableSet"
      }
)
@ProtoTypeId(ProtoStreamTypeIds.ADAPTER_SET)
public class SetAdapter {
   @ProtoFactory
   Set<?> create(MarshallableSet<?> elements) {
      return MarshallableSet.unwrap(elements);
   }

   @ProtoField(1)
   MarshallableSet<?> elements(Set<?> set) {
      return MarshallableSet.create(set);
   }
}
