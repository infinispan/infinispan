package org.infinispan.query.remote.impl.mapping.model;

import java.lang.invoke.MethodHandles;

import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.hibernate.search.mapper.pojo.model.spi.PojoGenericTypeModel;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeModel;
import org.hibernate.search.util.common.reflect.spi.ValueReadHandleFactory;
import org.infinispan.search.mapper.mapping.SearchMappingBuilder;

public class ProtobufBootstrapIntrospector implements PojoBootstrapIntrospector {

   private final PojoBootstrapIntrospector delegate = SearchMappingBuilder.introspector(MethodHandles.lookup());

   @Override
   public <T> PojoRawTypeModel<T> typeModel(Class<T> clazz) {
      return delegate.typeModel(clazz);
   }

   @Override
   public PojoRawTypeModel<?> typeModel(String name) {
      return new ProtobufRawTypeModel(typeModel(byte[].class), name);
   }

   @Override
   public <T> PojoGenericTypeModel<T> genericTypeModel(Class<T> clazz) {
      return delegate.genericTypeModel(clazz);
   }

   @Override
   public ValueReadHandleFactory annotationValueReadHandleFactory() {
      return delegate.annotationValueReadHandleFactory();
   }
}
