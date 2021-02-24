package org.infinispan.search.mapper.model.impl;

import org.hibernate.annotations.common.reflection.XProperty;
import org.hibernate.search.mapper.pojo.model.hcann.spi.AbstractPojoHCAnnRawTypeModel;
import org.hibernate.search.mapper.pojo.model.spi.GenericContextAwarePojoGenericTypeModel.RawTypeDeclaringContext;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;

import java.lang.reflect.Member;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

class InfinispanRawTypeModel<T> extends AbstractPojoHCAnnRawTypeModel<T, InfinispanBootstrapIntrospector> {

   InfinispanRawTypeModel(InfinispanBootstrapIntrospector introspector, PojoRawTypeIdentifier<T> typeIdentifier,
                          RawTypeDeclaringContext<T> rawTypeDeclaringContext) {
      super(introspector, typeIdentifier, rawTypeDeclaringContext);
   }

   @Override
   @SuppressWarnings("unchecked") // xClass represents T, so its supertypes represent ? super T
   public Stream<InfinispanRawTypeModel<? super T>> ascendingSuperTypes() {
      return introspector.ascendingSuperClasses(xClass)
            .map(xc -> (InfinispanRawTypeModel<? super T>) introspector.typeModel(xc));
   }

   @Override
   @SuppressWarnings("unchecked") // xClass represents T, so its supertypes represent ? super T
   public Stream<InfinispanRawTypeModel<? super T>> descendingSuperTypes() {
      return introspector.descendingSuperClasses(xClass)
            .map(xc -> (InfinispanRawTypeModel<? super T>) introspector.typeModel(xc));
   }

   @Override
   protected InfinispanPropertyModel<?> createPropertyModel(String propertyName) {
      List<XProperty> declaredXProperties = new ArrayList<>(2);
      List<XProperty> methodAccessXProperties = declaredMethodAccessXPropertiesByName().get(propertyName);
      if (methodAccessXProperties != null) {
         declaredXProperties.addAll(methodAccessXProperties);
      }
      XProperty fieldAccessXProperty = declaredFieldAccessXPropertiesByName().get(propertyName);
      if (fieldAccessXProperty != null) {
         declaredXProperties.add(fieldAccessXProperty);
      }

      List<Member> members = findPropertyMember(propertyName);
      if (members == null) {
         return null;
      }

      return new InfinispanPropertyModel<>(
            introspector, this, propertyName,
            declaredXProperties, members
      );
   }

   private List<Member> findPropertyMember(String propertyName) {
      // Try using the getter first (if declared)...
      List<Member> getters = findInSelfOrParents(t -> t.declaredPropertyGetters(propertyName));
      if (getters != null) {
         return getters;
      }
      // ... and fall back to the field (or null if not found)
      Member field = findInSelfOrParents(t -> t.declaredPropertyField(propertyName));
      return field == null ? null : Collections.singletonList(field);
   }

   private <T2> T2 findInSelfOrParents(Function<InfinispanRawTypeModel<?>, T2> getter) {
      return ascendingSuperTypes()
            .map(getter)
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
   }

}
