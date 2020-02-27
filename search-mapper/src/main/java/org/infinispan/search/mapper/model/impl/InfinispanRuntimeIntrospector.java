package org.infinispan.search.mapper.model.impl;

import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.model.spi.PojoRuntimeIntrospector;

public class InfinispanRuntimeIntrospector implements PojoRuntimeIntrospector {

   private final PojoRuntimeIntrospector delegate = PojoRuntimeIntrospector.simple();

   public InfinispanRuntimeIntrospector() {
   }

   @Override
   public <T> PojoRawTypeIdentifier<? extends T> detectEntityType(T entity) {
      return delegate.detectEntityType(entity);
   }

   @Override
   public Object unproxy(Object value) {
      return delegate.unproxy(value);
   }
}
