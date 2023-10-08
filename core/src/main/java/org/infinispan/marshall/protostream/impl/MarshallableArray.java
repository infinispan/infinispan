package org.infinispan.marshall.protostream.impl;

import static org.infinispan.marshall.protostream.impl.GlobalContextInitializer.getFqTypeName;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.ProtobufTagMarshaller;
import org.infinispan.protostream.TagReader;
import org.infinispan.protostream.TagWriter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.annotations.impl.GeneratedMarshallerBase;
import org.infinispan.protostream.descriptors.WireType;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A wrapper for arrays of objects whose type is unknown until runtime. This is equivalent to utilising a
 * <code>MarshallableObject[]</code> without the overhead of creating a {@link MarshallableObject} per
 * element.
 *
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MARSHALLABLE_ARRAY)
public class MarshallableArray<T> {

   static final Log log = LogFactory.getLog(MarshallableArray.class);

   /**
    * @param entries an array to be wrapped as a {@link MarshallableArray}.
    * @return a new {@link MarshallableArray} instance containing the passed object if the array is not null, otherwise
    * null.
    */
   public static <T> MarshallableArray<T> create(T[] entries) {
      return entries == null ? null : new MarshallableArray<>(entries);
   }

   /**
    * @param wrapper the {@link MarshallableArray} instance to unwrap
    * @return the wrapped array or null if the provided wrapper does not exist
    */
   public static <T> T[] unwrap(MarshallableArray<T> wrapper) {
      return wrapper == null ? null : wrapper.array;
   }

   /**
    * @param wrapper the {@link MarshallableArray} instance to unwrap
    * @param dest an array into which the array entries should be copied
    * @return the wrapped array or null if the provided wrapper does not exist
    */
   @SuppressWarnings("unchecked")
   public static <T> T[] unwrap(MarshallableArray<T> wrapper, T[] dest) {
      if (wrapper == null)
         return null;

      int size = wrapper.array.length;
      var array = wrapper.array;
      if (dest.length < size)
         return (T[]) Arrays.copyOf(array, size, dest.getClass());

      System.arraycopy(array, 0, dest, 0, size);
      if (dest.length > size)
         dest[size] = null;
      return dest;
   }

   private final T[] array;

   private MarshallableArray(T[] array) {
      this.array = array;
   }

   @ProtoFactory
   MarshallableArray(int size, List<byte[]> bytes) {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   @ProtoField(1)
   int size() {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   @ProtoField(2)
   List<byte[]> getBytes() {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   public Object[] get() {
      return array;
   }

   public static class Marshaller extends GeneratedMarshallerBase implements ProtobufTagMarshaller<MarshallableArray> {
      private final String typeName;
      private final AbstractInternalProtoStreamMarshaller marshaller;

      public Marshaller(AbstractInternalProtoStreamMarshaller marshaller) {
         this.typeName = getFqTypeName(MarshallableArray.class);
         this.marshaller = marshaller;
      }

      @Override
      public MarshallableArray read(ReadContext ctx) throws IOException {
         final TagReader in = ctx.getReader();
         int tag = in.readTag();
         if (tag == 0)
            return new MarshallableArray<>(new Object[0]);

         if (tag != (1 << WireType.TAG_TYPE_NUM_BITS | WireType.WIRETYPE_VARINT))
            throw new IllegalStateException("Unexpected tag: " + tag);

         int i = 0;
         final int size = in.readInt32();
         Object[] entries = new Object[size];
         boolean done = false;
         while (!done) {
            tag = in.readTag();
            switch (tag) {
               case 0:
                  done = true;
                  break;
               case 2 << WireType.TAG_TYPE_NUM_BITS | WireType.WIRETYPE_LENGTH_DELIMITED: {
                  byte[] bytes = in.readByteArray();
                  entries[i++] = marshaller.objectFromByteBuffer(bytes);
                  break;
               }
               default: {
                  if (!in.skipField(tag)) done = true;
               }
            }
         }
         return new MarshallableArray<>(entries);
      }

      @Override
      public void write(WriteContext ctx, MarshallableArray marshallableArray) throws IOException {
         Object[] array = marshallableArray.get();
         if (array != null && array.length > 0) {
            TagWriter writer = ctx.getWriter();
            writer.writeInt32(1, array.length);
            for (Object entry : array) {
               // Null objects are represented as 0 bytes by our internal Protostream marshaller implementations
               // All marshalling delegated to the user marshaller is wrapped in a MarshallableUserObject message, so
               // we know that the returned bytes can never be empty. This ensures that we can't misinterpret user
               // marshaller bytes as a null entry.
               ByteBuffer buf = marshaller.objectToBuffer(entry);
               writer.writeBytes(2, buf.getBuf(), buf.getOffset(), buf.getLength());
            }
         }
      }

      @Override
      public Class<? extends MarshallableArray> getJavaClass() {
         return MarshallableArray.class;
      }

      @Override
      public String getTypeName() {
         return typeName;
      }
   }
}
