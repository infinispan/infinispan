package org.infinispan.search.mapper.model.impl;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.hibernate.annotations.common.reflection.XClass;
import org.hibernate.annotations.common.reflection.XProperty;
import org.hibernate.search.engine.mapper.model.spi.MappableTypeModel;
import org.hibernate.search.mapper.pojo.model.spi.GenericContextAwarePojoGenericTypeModel.RawTypeDeclaringContext;
import org.hibernate.search.mapper.pojo.model.spi.JavaClassPojoCaster;
import org.hibernate.search.mapper.pojo.model.spi.PojoCaster;
import org.hibernate.search.mapper.pojo.model.spi.PojoPropertyModel;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeModel;
import org.hibernate.search.util.common.logging.impl.LoggerFactory;
import org.infinispan.search.mapper.log.impl.Log;

class InfinispanTypeModel<T> implements PojoRawTypeModel<T> {

   private static final Log log = LoggerFactory.make(Log.class, MethodHandles.lookup());

   private final InfinispanBootstrapIntrospector introspector;
   private final PojoRawTypeIdentifier<T> typeIdentifier;
   private final RawTypeDeclaringContext<T> rawTypeDeclaringContext;
   private final PojoCaster<T> caster;
   private final XClass xClass;
   private final Map<String, XProperty> declaredProperties;

   InfinispanTypeModel(InfinispanBootstrapIntrospector introspector, PojoRawTypeIdentifier<T> typeIdentifier,
                       RawTypeDeclaringContext<T> rawTypeDeclaringContext) {
      this.introspector = introspector;
      this.typeIdentifier = typeIdentifier;
      this.rawTypeDeclaringContext = rawTypeDeclaringContext;
      this.caster = new JavaClassPojoCaster<>(typeIdentifier.javaClass());
      this.xClass = introspector.toXClass(typeIdentifier.javaClass());
      this.declaredProperties = introspector.declaredMethodAccessXPropertiesByName(xClass);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      InfinispanTypeModel<?> that = (InfinispanTypeModel<?>) o;
      /*
       * We need to take the introspector into account, so that the engine does not confuse
       * type models from different mappers during bootstrap.
       */
      return Objects.equals(introspector, that.introspector) && Objects.equals(typeIdentifier, that.typeIdentifier);
   }

   @Override
   public int hashCode() {
      return Objects.hash(introspector, typeIdentifier);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "[" + typeIdentifier + "]";
   }

   @Override
   public PojoRawTypeIdentifier<T> typeIdentifier() {
      return typeIdentifier;
   }

   @Override
   public String name() {
      return typeIdentifier.toString();
   }

   @Override
   public boolean isAbstract() {
      return Modifier.isAbstract(typeIdentifier.javaClass().getModifiers());
   }

   @Override
   public boolean isSubTypeOf(MappableTypeModel other) {
      return other instanceof InfinispanTypeModel && ((InfinispanTypeModel<?>) other).typeIdentifier.javaClass()
            .isAssignableFrom(typeIdentifier.javaClass());
   }

   @Override
   public PojoRawTypeModel<T> rawType() {
      return this;
   }

   @Override
   @SuppressWarnings("unchecked") // xClass represents T, so its supertypes represent ? super T
   public Stream<InfinispanTypeModel<? super T>> ascendingSuperTypes() {
      return (Stream<InfinispanTypeModel<? super T>>) introspector.ascendingSuperTypes(xClass);
   }

   @Override
   @SuppressWarnings("unchecked") // xClass represents T, so its supertypes represent ? super T
   public Stream<? extends PojoRawTypeModel<? super T>> descendingSuperTypes() {
      return (Stream<? extends PojoRawTypeModel<? super T>>) introspector.descendingSuperTypes(xClass);
   }

   @Override
   public Stream<Annotation> annotations() {
      return introspector.annotations(xClass);
   }

   @Override
   public PojoPropertyModel<?> property(String propertyName) {
      return ascendingSuperTypes().map(model -> model.declaredProperties.get(propertyName))
            .filter(Objects::nonNull).findFirst().map(this::createProperty).orElseThrow(
                  () -> log.cannotFindProperty(this, propertyName));
   }

   @Override
   public Stream<PojoPropertyModel<?>> declaredProperties() {
      return declaredProperties.values().stream().map(this::createProperty);
   }

   @Override
   public PojoCaster<T> caster() {
      return caster;
   }

   RawTypeDeclaringContext<T> getRawTypeDeclaringContext() {
      return rawTypeDeclaringContext;
   }

   private PojoPropertyModel<?> createProperty(XProperty property) {
      return new InfinispanPropertyModel<>(introspector, this, property);
   }
}
