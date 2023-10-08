package org.infinispan.marshall.protostream.impl;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.descriptors.WireType;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An abstract class which provides the basis of wrapper implementations which need to delegate the marshalling of an
 * Object to a {@link org.infinispan.commons.marshall.Marshaller} implementation at runtime.
 * <p>
 * This abstraction hides the details of the configured marshaller from our internal Pojos, so that all calls to the
 * marshaller required by the implementation class can be limited to the {@link AbstractMarshallableWrapper}
 * implementation.
 *
 * @author Ryan Emerson
 * @since 16.0
 */
abstract class AbstractMarshallableWrapper<T> {

   static final Log log = LogFactory.getLog(MarshallableMap.class);

   protected final T object;

   protected AbstractMarshallableWrapper(byte[] bytes) {
      // no-op never actually used, as we override the default marshaller
      throw log.marshallerNotOverridden(getClass().getName());
   }

   protected AbstractMarshallableWrapper(T object) {
      this.object = object;
   }

   @ProtoField(1)
   byte[] getBytes() {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   public T get() {
      return object;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AbstractMarshallableWrapper other = (AbstractMarshallableWrapper) o;
      return Objects.equals(object, other.object);
   }

   @Override
   public int hashCode() {
      return object != null ? object.hashCode() : 0;
   }

   public static int size(int objectBytes) {
      int typeId = ProtoStreamTypeIds.MARSHALLABLE_USER_OBJECT;
      int typeIdSize = tagSize(19, 1) + computeUInt32SizeNoTag(typeId);
      int userBytesFieldSize = tagSize(1, 2) + computeUInt32SizeNoTag(objectBytes) + objectBytes;
      int wrappedMessageSize = tagSize(17, 2) + computeUInt32SizeNoTag(objectBytes);

      return typeIdSize + userBytesFieldSize + wrappedMessageSize;
   }

   private static int tagSize(int fieldNumber, int wireType) {
      return computeUInt32SizeNoTag(fieldNumber << WireType.TAG_TYPE_NUM_BITS | wireType);
   }

   // Protobuf logic included to avoid requiring a dependency on com.google.protobuf.CodedOutputStream
   private static int computeUInt32SizeNoTag(int value) {
      if ((value & -128) == 0) {
         return 1;
      } else if ((value & -16384) == 0) {
         return 2;
      } else if ((value & -2097152) == 0) {
         return 3;
      } else {
         return (value & -268435456) == 0 ? 4 : 5;
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "object=" + Util.toStr(object) +
            '}';
   }
}
