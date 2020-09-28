package org.infinispan.search.mapper.model.impl;

import java.lang.reflect.Member;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import org.hibernate.annotations.common.reflection.XProperty;
import org.hibernate.search.mapper.pojo.model.hcann.spi.AbstractPojoHCAnnRawTypeModel;
import org.hibernate.search.mapper.pojo.model.spi.GenericContextAwarePojoGenericTypeModel.RawTypeDeclaringContext;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;

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
      XProperty methodAccessXProperty = declaredMethodAccessXPropertiesByName().get(propertyName);
      if (methodAccessXProperty != null) {
         declaredXProperties.add(methodAccessXProperty);
      }
      XProperty fieldAccessXProperty = declaredFieldAccessXPropertiesByName().get(propertyName);
      if (fieldAccessXProperty != null) {
         declaredXProperties.add(fieldAccessXProperty);
      }

      Member member = findPropertyMember(propertyName);
      if (member == null) {
         return null;
      }

      return new InfinispanPropertyModel<>(
            introspector, this, propertyName,
            declaredXProperties, member
      );
   }

   private Member findPropertyMember(String propertyName) {
      // Try using the getter first (if declared)...
      Member getter = findInSelfOrParents(t -> t.declaredPropertyGetter(propertyName));
      if (getter != null) {
         return getter;
      }
      // ... and fall back to the field (or null if not found)
      return findInSelfOrParents(t -> t.declaredPropertyField(propertyName));
   }

   private <T2> T2 findInSelfOrParents(Function<InfinispanRawTypeModel<?>, T2> getter) {
      return ascendingSuperTypes()
            .map(getter)
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
   }

}
