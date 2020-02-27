package org.infinispan.search.mapper.model.impl;

import java.lang.reflect.Type;

import org.hibernate.search.mapper.pojo.model.spi.GenericContextAwarePojoGenericTypeModel;
import org.hibernate.search.mapper.pojo.model.spi.PojoPropertyModel;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeModel;

class InfinispanGenericContextHelper implements GenericContextAwarePojoGenericTypeModel.Helper {
   private final InfinispanBootstrapIntrospector introspector;

   public InfinispanGenericContextHelper(InfinispanBootstrapIntrospector introspector) {
      this.introspector = introspector;
   }

   @Override
   public <T> PojoRawTypeModel<T> rawTypeModel(Class<T> clazz) {
      return introspector.typeModel(clazz);
   }

   @Override
   public Object propertyCacheKey(PojoPropertyModel<?> rawPropertyModel) {
      return rawPropertyModel; // Properties define equals and hashCode as required
   }

   @Override
   public Type propertyGenericType(PojoPropertyModel<?> rawPropertyModel) {
      InfinispanPropertyModel<?> infinispanPropertyModel = (InfinispanPropertyModel<?>) rawPropertyModel;
      return infinispanPropertyModel.getGetterGenericReturnType();
   }
}
