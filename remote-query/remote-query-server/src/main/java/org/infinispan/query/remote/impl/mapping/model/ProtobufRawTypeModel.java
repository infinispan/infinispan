package org.infinispan.query.remote.impl.mapping.model;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Stream;

import org.hibernate.search.engine.mapper.model.spi.MappableTypeModel;
import org.hibernate.search.mapper.pojo.model.spi.JavaClassPojoCaster;
import org.hibernate.search.mapper.pojo.model.spi.PojoCaster;
import org.hibernate.search.mapper.pojo.model.spi.PojoPropertyModel;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeModel;

public class ProtobufRawTypeModel implements PojoRawTypeModel<byte[]> {

   private final PojoRawTypeIdentifier<byte[]> typeIdentifier;
   private final PojoCaster<byte[]> caster;

   public ProtobufRawTypeModel(String name) {
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
      // Protocol Buffers does not support inheritance.
      return equals(superTypeCandidate);
   }

   @Override
   public Stream<? extends PojoRawTypeModel<? super byte[]>> ascendingSuperTypes() {
      // Protocol Buffers does not support inheritance.
      return Stream.of(this);
   }

   @Override
   public Stream<? extends PojoRawTypeModel<? super byte[]>> descendingSuperTypes() {
      // Protocol Buffers does not support inheritance.
      return Stream.of(this);
   }

   @Override
   public Stream<Annotation> annotations() {
      // Mapping is defined programmatically (at the moment)
      return Stream.empty();
   }

   @Override
   public Collection<PojoPropertyModel<?>> declaredProperties() {
      // Properties are created by ProtobufMessageBinder
      return Collections.emptySet();
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
