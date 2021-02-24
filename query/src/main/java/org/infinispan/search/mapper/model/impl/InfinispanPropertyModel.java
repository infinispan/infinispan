package org.infinispan.search.mapper.model.impl;

import java.lang.reflect.Member;
import java.util.List;

import org.hibernate.annotations.common.reflection.XProperty;
import org.hibernate.search.mapper.pojo.model.hcann.spi.AbstractPojoHCAnnPropertyModel;
import org.hibernate.search.util.common.reflect.spi.ValueReadHandle;

class InfinispanPropertyModel<T> extends AbstractPojoHCAnnPropertyModel<T, InfinispanBootstrapIntrospector> {

   InfinispanPropertyModel(InfinispanBootstrapIntrospector introspector,
                           InfinispanRawTypeModel<?> holderTypeModel,
                           String name, List<XProperty> declaredXProperties,
                           List<Member> members) {
      super(introspector, holderTypeModel, name, declaredXProperties, members);
   }

   @Override
   @SuppressWarnings("unchecked") // By construction, we know the member returns values of type T
   protected ValueReadHandle<T> createHandle(Member member) throws IllegalAccessException {
      return (ValueReadHandle<T>) introspector.createValueReadHandle(member);
   }
}
