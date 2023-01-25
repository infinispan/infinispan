package org.infinispan.query.remote.impl.mapping.model;

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

public class ProtobufRawTypeModel implements PojoRawTypeModel<byte[]> {

   private final PojoRawTypeModel<byte[]> superType;
   private final PojoRawTypeIdentifier<byte[]> typeIdentifier;
   private final PojoCaster<byte[]> caster;

   public ProtobufRawTypeModel(PojoRawTypeModel<byte[]> superType, String name) {
      this.superType = superType;
      this.typeIdentifier = PojoRawTypeIdentifier.of(byte[].class, name);
      this.caster = new JavaClassPojoCaster<>(byte[].class);
   }

   @Override
   public PojoRawTypeIdentifier<byte[]> typeIdentifier() {
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
   public Stream<? extends PojoRawTypeModel<? super byte[]>> ascendingSuperTypes() {
      return Stream.concat(Stream.of(this), superType.ascendingSuperTypes());
   }

   @Override
   public Stream<? extends PojoRawTypeModel<? super byte[]>> descendingSuperTypes() {
      return Stream.concat(superType.descendingSuperTypes(), Stream.of(this));
   }

   @Override
   public Stream<Annotation> annotations() {
      // Mapping is defined programmatically (at the moment)
      return Stream.empty();
   }

   @Override
   public PojoConstructorModel<byte[]> mainConstructor() {
      return null;
   }

   @Override
   public PojoConstructorModel<byte[]> constructor(Class<?>... parameterTypes) {
      return null;
   }

   @Override
   public Collection<PojoConstructorModel<byte[]>> declaredConstructors() {
      // No support for constructors on dynamic-map types.
      return Collections.emptyList();
   }

   @Override
   public Collection<PojoPropertyModel<?>> declaredProperties() {
      // Properties are created by ProtobufMessageBinder
      return Collections.emptySet();
   }

   @Override
   public PojoTypeModel<? extends byte[]> cast(PojoTypeModel<?> other) {
      if ( other.rawType().isSubTypeOf( this ) ) {
         // Redundant cast; no need to create a new type.
         return (PojoTypeModel<? extends byte[]>) other;
      }
      else {
         // There is no generic type information to retain for protobuf types; we can just return this.
         // Also, calling other.castTo(...) would mean losing the type name, and we definitely don't want that.
         return this;
      }
   }

   @Override
   public PojoCaster<byte[]> caster() {
      return caster;
   }

   @Override
   public String name() {
      return typeIdentifier.toString();
   }

   @Override
   public PojoRawTypeModel<byte[]> rawType() {
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
      return new StringJoiner(", ", ProtobufRawTypeModel.class.getSimpleName() + "[", "]").add("typeIdentifier=" + typeIdentifier).toString();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      ProtobufRawTypeModel that = (ProtobufRawTypeModel) o;
      return Objects.equals(typeIdentifier, that.typeIdentifier);
   }

   @Override
   public int hashCode() {
      return Objects.hash(typeIdentifier);
   }
}
