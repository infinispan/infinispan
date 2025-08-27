package org.infinispan.server.core.query.impl.mapping.model;

import java.lang.invoke.MethodHandles;

import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeModel;
import org.hibernate.search.util.common.reflect.spi.ValueHandleFactory;
import org.infinispan.server.core.query.impl.mapping.reference.GlobalReferenceHolder;
import org.infinispan.server.core.query.impl.mapping.type.ProtobufKeyValuePair;
import org.infinispan.query.mapper.mapping.SearchMappingBuilder;

public class ProtobufBootstrapIntrospector implements PojoBootstrapIntrospector {

   private final PojoBootstrapIntrospector delegate = SearchMappingBuilder.introspector(MethodHandles.lookup());
   private final GlobalReferenceHolder globalReferenceHolder;

   public ProtobufBootstrapIntrospector(GlobalReferenceHolder globalReferenceHolder) {
      this.globalReferenceHolder = globalReferenceHolder;
   }

   @Override
   public <T> PojoRawTypeModel<T> typeModel(Class<T> clazz) {
      return delegate.typeModel(clazz);
   }

   @Override
   public PojoRawTypeModel<?> typeModel(String name) {
      return (globalReferenceHolder.hasKeyMapping(name)) ?
            new ProtobufKeyValueTypeModel(typeModel(ProtobufKeyValuePair.class), name) :
            new ProtobufRawTypeModel(typeModel(byte[].class), name);
   }

   @Override
   public ValueHandleFactory annotationValueHandleFactory() {
      return delegate.annotationValueHandleFactory();
   }

}
