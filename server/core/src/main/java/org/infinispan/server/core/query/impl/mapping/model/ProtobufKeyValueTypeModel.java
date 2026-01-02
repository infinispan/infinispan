package org.infinispan.server.core.query.impl.mapping.model;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Stream;

import org.hibernate.search.engine.mapper.model.spi.MappableTypeModel;
import org.hibernate.search.mapper.pojo.model.spi.JavaClassPojoCaster;
import org.hibernate.search.mapper.pojo.model.spi.PojoCaster;
import org.hibernate.search.mapper.pojo.model.spi.PojoConstructorModel;
import org.hibernate.search.mapper.pojo.model.spi.PojoPropertyModel;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeModel;
import org.hibernate.search.mapper.pojo.model.spi.PojoTypeModel;
import org.infinispan.server.core.query.impl.mapping.type.ProtobufKeyValuePair;

public class ProtobufKeyValueTypeModel implements PojoRawTypeModel<ProtobufKeyValuePair> {

   private final PojoRawTypeModel<ProtobufKeyValuePair> superType;
   private final PojoRawTypeIdentifier<ProtobufKeyValuePair> typeIdentifier;
   private final PojoCaster<ProtobufKeyValuePair> caster;

   public ProtobufKeyValueTypeModel(PojoRawTypeModel<ProtobufKeyValuePair> superType, String name) {
      this.superType = superType;
      this.typeIdentifier = PojoRawTypeIdentifier.of(ProtobufKeyValuePair.class, name);
      this.caster = new JavaClassPojoCaster<>(ProtobufKeyValuePair.class);
   }

   @Override
   public PojoRawTypeIdentifier<ProtobufKeyValuePair> typeIdentifier() {
      return typeIdentifier;
   }

   @Override
   public boolean isAbstract() {
      // Protocol Buffers does not support abstract classes.
      return false;
   }

   @Override
   public boolean isSubTypeOf(MappableTypeModel superTypeCandidate) {
      return equals(superTypeCandidate) || superType.isSubTypeOf(superTypeCandidate);
   }

   @Override
   public Stream<? extends PojoRawTypeModel<? super ProtobufKeyValuePair>> ascendingSuperTypes() {
      return Stream.concat(Stream.of(this), superType.ascendingSuperTypes());
   }

   @Override
   public Stream<? extends PojoRawTypeModel<? super ProtobufKeyValuePair>> descendingSuperTypes() {
      return Stream.concat(superType.descendingSuperTypes(), Stream.of(this));
   }

   @Override
   public Stream<Annotation> annotations() {
      // Mapping is defined programmatically (at the moment)
      return Stream.empty();
   }

   @Override
   public PojoConstructorModel<ProtobufKeyValuePair> mainConstructor() {
      return null;
   }

   @Override
   public PojoConstructorModel<ProtobufKeyValuePair> constructor(Class<?>... parameterTypes) {
      return null;
   }

   @Override
   public Collection<PojoConstructorModel<ProtobufKeyValuePair>> declaredConstructors() {
      // No support for constructors on dynamic-map types.
      return Collections.emptyList();
   }

   @Override
   public Collection<PojoPropertyModel<?>> declaredProperties() {
      // Properties are created by ProtobufMessageBinder
      return Collections.emptySet();
   }

   @Override
   public PojoTypeModel<? extends ProtobufKeyValuePair> cast(PojoTypeModel<?> other) {
      if (other.rawType().isSubTypeOf(this)) {
         // Redundant cast; no need to create a new type.
         return (PojoTypeModel<? extends ProtobufKeyValuePair>) other;
      } else {
         // There is no generic type information to retain for protobuf types; we can just return this.
         // Also, calling other.castTo(...) would mean losing the type name, and we definitely don't want that.
         return this;
      }
   }

   @Override
   public PojoCaster<ProtobufKeyValuePair> caster() {
      return caster;
   }

   @Override
   public String name() {
      return typeIdentifier.toString();
   }

   @Override
   public PojoRawTypeModel<ProtobufKeyValuePair> rawType() {
      return this;
   }

   @Override
   public PojoPropertyModel<?> property(String propertyName) {
      // Properties are created by ProtobufMessageBinder
      return null;
   }

   @Override
   public <U> Optional<PojoTypeModel<? extends U>> castTo(Class<U> target) {
      return Optional.empty();
   }

   @Override
   public Optional<? extends PojoTypeModel<?>> typeArgument(Class<?> rawSuperType, int typeParameterIndex) {
      return Optional.empty();
   }

   @Override
   public Optional<? extends PojoTypeModel<?>> arrayElementType() {
      return Optional.empty();
   }

   @Override
   public String toString() {
      return new StringJoiner(", ", ProtobufKeyValueTypeModel.class.getSimpleName() + "[", "]").add("typeIdentifier=" + typeIdentifier).toString();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      ProtobufKeyValueTypeModel that = (ProtobufKeyValueTypeModel) o;
      return Objects.equals(typeIdentifier, that.typeIdentifier);
   }

   @Override
   public int hashCode() {
      return Objects.hash(typeIdentifier);
   }
}
