package org.infinispan.query.impl.protostream.adapters;

import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(PojoRawTypeIdentifier.class)
@ProtoTypeId(ProtoStreamTypeIds.HIBERNATE_POJO_RAW_TYPE_IDENTIFIER)
public class HibernatePojoRawTypeIdentifierAdapter {

   @ProtoFactory
   static PojoRawTypeIdentifier<?> protoFactory(Class<?> clazz, String typeName) {
      return PojoRawTypeIdentifier.of(clazz, typeName);
   }

   @ProtoField(number = 1, name = "class")
   Class<?> getClazz(PojoRawTypeIdentifier<?> pojoRawTypeIdentifier) {
      return pojoRawTypeIdentifier.javaClass();
   }

   @ProtoField(2)
   String getTypeName(PojoRawTypeIdentifier<?> pojoRawTypeIdentifier) {
      return pojoRawTypeIdentifier.isNamed() ? (String) ReflectionUtil.getValue(pojoRawTypeIdentifier, "name") : null;
   }
}
