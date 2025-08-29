package org.infinispan.query.mapper.model.impl;

import java.lang.reflect.Member;
import java.util.List;

import org.hibernate.models.spi.MemberDetails;
import org.hibernate.search.mapper.pojo.model.models.spi.AbstractPojoModelsPropertyModel;
import org.hibernate.search.util.common.reflect.spi.ValueReadHandle;

class InfinispanPropertyModel<T> extends AbstractPojoModelsPropertyModel<T, InfinispanBootstrapIntrospector> {

   InfinispanPropertyModel(InfinispanBootstrapIntrospector introspector,
                           InfinispanRawTypeModel<?> holderTypeModel,
                           String name, List<MemberDetails> declaredProperties,
                           List<Member> members) {
      super(introspector, holderTypeModel, name, declaredProperties, members);
   }

   @Override
   @SuppressWarnings("unchecked") // By construction, we know the member returns values of type T
   protected ValueReadHandle<T> createHandle(Member member) throws IllegalAccessException {
      return (ValueReadHandle<T>) introspector.createValueReadHandle(member);
   }
}
