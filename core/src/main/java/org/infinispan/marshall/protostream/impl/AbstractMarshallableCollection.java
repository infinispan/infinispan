package org.infinispan.marshall.protostream.impl;

import static org.infinispan.marshall.protostream.impl.GlobalContextInitializer.getFqTypeName;

import java.io.IOException;
import java.util.Collection;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.protostream.ProtobufTagMarshaller;
import org.infinispan.protostream.TagReader;
import org.infinispan.protostream.TagWriter;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.impl.GeneratedMarshallerBase;
import org.infinispan.protostream.descriptors.WireType;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
abstract class AbstractMarshallableCollection<T> {

   static final Log log = LogFactory.getLog(AbstractMarshallableCollection.class);

   @ProtoField(1)
   int getSize() {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   abstract Collection<T> get();

   static abstract class Marshaller extends GeneratedMarshallerBase implements ProtobufTagMarshaller<AbstractMarshallableCollection> {
      private final String typeName;
      private final AbstractInternalProtoStreamMarshaller marshaller;

      protected Marshaller(AbstractInternalProtoStreamMarshaller marshaller) {
         this.marshaller = marshaller;
         this.typeName = getFqTypeName(getJavaClass());
      }

      abstract Collection<Object> newCollection(int size);
      abstract AbstractMarshallableCollection<?> newWrapperInstance(Collection<?> o);

      @Override
      public AbstractMarshallableCollection read(ReadContext ctx) throws IOException {
         final TagReader in = ctx.getReader();
         int tag = in.readTag();
         if (tag == 0)
            return newWrapperInstance(newCollection(0));

         if (tag != (1 << WireType.TAG_TYPE_NUM_BITS | WireType.WIRETYPE_VARINT))
            throw new IllegalStateException("Unexpected tag: " + tag);

         final int size = in.readInt32();
         Collection<Object> entries = newCollection(size);
         boolean done = false;
         while (!done) {
            tag = in.readTag();
            switch (tag) {
               case 0:
                  done = true;
                  break;
               case 2 << WireType.TAG_TYPE_NUM_BITS | WireType.WIRETYPE_LENGTH_DELIMITED: {
                  byte[] bytes = in.readByteArray();
                  entries.add(marshaller.objectFromByteBuffer(bytes));
                  break;
               }
               default: {
                  if (!in.skipField(tag)) done = true;
               }
            }
         }
         return newWrapperInstance(entries);
      }

      @Override
      public void write(WriteContext ctx, AbstractMarshallableCollection abstractMarshallableCollection) throws IOException {
         Collection<?> collection = abstractMarshallableCollection.get();
         if (collection != null && !collection.isEmpty()) {
            TagWriter writer = ctx.getWriter();
            writer.writeInt32(1, collection.size());
            for (Object entry : collection) {
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
      public String getTypeName() {
         return typeName;
      }
   }
}
