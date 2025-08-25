package org.infinispan.marshall.protostream.impl;

import static org.infinispan.marshall.protostream.impl.GlobalContextInitializer.getFqTypeName;

import java.io.IOException;
import java.util.Collection;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.protostream.BaseMarshallerDelegate;
import org.infinispan.protostream.GeneratedMarshallerBase;
import org.infinispan.protostream.ProtobufTagMarshaller;
import org.infinispan.protostream.TagReader;
import org.infinispan.protostream.TagWriter;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoField;
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

   abstract static class Marshaller extends GeneratedMarshallerBase implements ProtobufTagMarshaller<AbstractMarshallableCollection> {
      private final String typeName;
      private final GlobalMarshaller marshaller;

      private volatile BaseMarshallerDelegate<WrappedMessage> delegate;

      protected Marshaller(GlobalMarshaller marshaller) {
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
                  var buf = in.readByteBuffer();
                  entries.add(marshaller.objectFromByteBuffer(buf));
                  break;
               }
               case 3 << WireType.TAG_TYPE_NUM_BITS | WireType.WIRETYPE_LENGTH_DELIMITED: {
                  if (delegate == null)
                     delegate = ctx.getSerializationContext().getMarshallerDelegate(org.infinispan.protostream.WrappedMessage.class);
                  int length = in.readUInt32();
                  int oldLimit = in.pushLimit(length);
                  WrappedMessage message = readMessage(delegate, ctx);
                  entries.add(message.getValue());
                  in.checkLastTagWas(0);
                  in.popLimit(oldLimit);
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
               if (entry == null || !marshaller.isMarshallableWithoutWrapping(entry)) {
                  ByteBuffer buf = marshaller.objectToBuffer(entry);
                  writer.writeBytes(2, buf.getBuf(), buf.getOffset(), buf.getLength());
               } else {
                  if (delegate == null)
                     delegate = ctx.getSerializationContext().getMarshallerDelegate(WrappedMessage.class);
                  writeNestedMessage(delegate, ctx, 3, new WrappedMessage(entry));
               }
            }
         }
      }

      @Override
      public String getTypeName() {
         return typeName;
      }
   }
}
