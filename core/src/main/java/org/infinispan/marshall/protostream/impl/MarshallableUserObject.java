package org.infinispan.marshall.protostream.impl;

import java.io.IOException;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.RawProtoStreamReader;
import org.infinispan.protostream.RawProtoStreamWriter;
import org.infinispan.protostream.RawProtobufMarshaller;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A wrapper message used by ProtoStream Marshallers to allow user objects to be marshalled/unmarshalled by delegating
 * to the configured user marshaller in {@link org.infinispan.configuration.global.SerializationConfiguration} if it
 * exists.
 * <p>
 * This abstraction hides the details of the configured user marshaller from our internal Pojos.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MARSHALLABLE_USER_OBJECT)
public final class MarshallableUserObject<T> {

   private T object;

   @ProtoField(number = 1)
   byte[] bytes;

   @ProtoFactory
   MarshallableUserObject(byte[] bytes) {
      this.bytes = bytes;
   }

   public MarshallableUserObject(T object) {
      this.object = object;
   }

   public T getObject() {
      return object;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MarshallableUserObject other = (MarshallableUserObject) o;
      return object != null ? object.equals(other.object) : other.object == null;
   }

   @Override
   public int hashCode() {
      return object != null ? object.hashCode() : 0;
   }

   public static int size(int objectBytes) {
      int typeIdSize = tagSize(19, 1) + computeUInt32SizeNoTag(ProtoStreamTypeIds.MARSHALLABLE_USER_OBJECT);
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

   public static BaseMarshaller<MarshallableUserObject> makeMarshaller(RawProtobufMarshaller<MarshallableUserObject> protobufMarshaller, Marshaller userMarshaller) {
      return new RawProtobufMarshaller<MarshallableUserObject>() {

         @Override
         public Class<? extends MarshallableUserObject> getJavaClass() {
            return protobufMarshaller.getJavaClass();
         }

         @Override
         public String getTypeName() {
            return protobufMarshaller.getTypeName();
         }

         @Override
         public MarshallableUserObject readFrom(ImmutableSerializationContext ctx, RawProtoStreamReader in) throws IOException {
            MarshallableUserObject marshallableUserObject = protobufMarshaller.readFrom(ctx, in);
            try {
               marshallableUserObject.object = userMarshaller.objectFromByteBuffer(marshallableUserObject.bytes);
               marshallableUserObject.bytes = null;
            } catch (ClassNotFoundException e) {
               throw new MarshallingException(e);
            }
            return marshallableUserObject;
         }

         @Override
         public void writeTo(ImmutableSerializationContext ctx, RawProtoStreamWriter out, MarshallableUserObject marshallableUserObject) throws IOException {
            try {
               marshallableUserObject.bytes = userMarshaller.objectToByteBuffer(marshallableUserObject.object);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new MarshallingException(e);
            }
            protobufMarshaller.writeTo(ctx, out, marshallableUserObject);
            marshallableUserObject.bytes = null;
         }
      };
   }
}
