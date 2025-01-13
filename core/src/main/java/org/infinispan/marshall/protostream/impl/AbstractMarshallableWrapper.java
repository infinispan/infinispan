package org.infinispan.marshall.protostream.impl;

import java.io.IOException;
import java.util.Objects;

import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.ProtobufTagMarshaller;
import org.infinispan.protostream.TagReader;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.impl.GeneratedMarshallerBase;
import org.infinispan.protostream.descriptors.WireType;

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

   protected final T object;

   protected AbstractMarshallableWrapper(byte[] bytes) {
      // no-op never actually used, as we override the default marshaller
      throw new IllegalStateException(this.getClass().getSimpleName() + " marshaller not overridden in SerializationContext");
   }

   protected AbstractMarshallableWrapper(T object) {
      this.object = object;
   }

   @ProtoField(number = 1)
   byte[] getBytes() {
      return Util.EMPTY_BYTE_ARRAY;
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
      return computeUInt32SizeNoTag(fieldNumber << 3 | wireType);
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

   protected abstract static class Marshaller extends GeneratedMarshallerBase implements ProtobufTagMarshaller<AbstractMarshallableWrapper<Object>> {

      private final String typeName;
      private final org.infinispan.commons.marshall.Marshaller marshaller;

      public Marshaller(String typeName, org.infinispan.commons.marshall.Marshaller marshaller) {
         this.typeName = typeName;
         this.marshaller = marshaller;
      }

      @Override
      public String getTypeName() {
         return typeName;
      }

      abstract AbstractMarshallableWrapper<Object> newWrapperInstance(Object o);

      @Override
      public AbstractMarshallableWrapper<Object> read(ReadContext ctx) throws IOException {
         final TagReader in = ctx.getReader();
         try {
            byte[] bytes = null;
            boolean done = false;
            while (!done) {
               final int tag = in.readTag();
               switch (tag) {
                  case 0:
                     done = true;
                     break;
                  case 1 << 3 | WireType.WIRETYPE_LENGTH_DELIMITED: {
                     bytes = in.readByteArray();
                     break;
                  }
                  default: {
                     if (!in.skipField(tag)) done = true;
                  }
               }
            }
            // TODO add method to ProtoStream marshallers to prevent MarshallableUserObject being unwrapped and then wrapped again here?
            Object userObject = bytes == null ? null : marshaller.objectFromByteBuffer(bytes);
            return newWrapperInstance(userObject);
         } catch (ClassNotFoundException e) {
            throw new MarshallingException(e);
         }
      }

      @Override
      public void write(WriteContext ctx, AbstractMarshallableWrapper<Object> wrapper) throws IOException {
         try {
            Object object = wrapper.get();
            if (object == null)
               return;

            byte[] bytes = marshaller.objectToByteBuffer(object);
            ctx.getWriter().writeBytes(1, bytes);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MarshallingException(e);
         }
      }
   }
}
