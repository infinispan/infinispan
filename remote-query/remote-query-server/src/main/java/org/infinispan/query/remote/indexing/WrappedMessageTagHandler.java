package org.infinispan.query.remote.indexing;

import com.google.protobuf.Descriptors;
import org.apache.lucene.document.Document;
import org.hibernate.search.bridge.LuceneOptions;
import org.infinispan.commons.CacheException;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.TagHandler;
import org.infinispan.query.remote.QueryFacadeImpl;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class WrappedMessageTagHandler implements TagHandler {

   private static final int wrappedDouble = 1;
   private static final int wrappedFloat = 2;
   private static final int wrappedInt64 = 3;
   private static final int wrappedUInt64 = 4;
   private static final int wrappedInt32 = 5;
   private static final int wrappedFixed64 = 6;
   private static final int wrappedFixed32 = 7;
   private static final int wrappedBool = 8;
   private static final int wrappedString = 9;
   private static final int wrappedBytes = 10;
   private static final int wrappedUInt32 = 11;
   private static final int wrappedSFixed32 = 12;
   private static final int wrappedSFixed64 = 13;
   private static final int wrappedSInt32 = 14;
   private static final int wrappedSInt64 = 15;
   private static final int wrappedDescriptorFullName = 16;
   private static final int wrappedMessageBytes = 17;
   private static final int wrappedEnum = 18;

   private final Document document;
   private final LuceneOptions luceneOptions;
   private final SerializationContext serCtx;

   private Descriptors.Descriptor messageDescriptor;

   public WrappedMessageTagHandler(Document document, LuceneOptions luceneOptions, SerializationContext serCtx) {
      this.document = document;
      this.luceneOptions = luceneOptions;
      this.serCtx = serCtx;
   }

   @Override
   public void onStart() {
   }

   @Override
   public void onTag(int fieldNumber, String fieldName, Descriptors.FieldDescriptor.Type type, Descriptors.FieldDescriptor.JavaType javaType, Object value) {
      switch (fieldNumber) {
         case wrappedDouble:
         case wrappedFloat:
         case wrappedInt64:
         case wrappedUInt64:
         case wrappedInt32:
         case wrappedFixed64:
         case wrappedFixed32:
         case wrappedBool:
         case wrappedString:
         case wrappedBytes:
         case wrappedUInt32:
         case wrappedSFixed32:
         case wrappedSFixed64:
         case wrappedSInt32:
         case wrappedSInt64:
         case wrappedEnum:    // todo [anistor] handle Enums by indexing the name of the value rather than the numeric value
            //todo [anistor] how do we index a scalar value?
            luceneOptions.addFieldToDocument("theValue", String.valueOf(value), document);
            break;
         case wrappedDescriptorFullName:
            messageDescriptor = serCtx.getMessageDescriptor((String) value);
            break;
         //todo [anistor] tag ordering could be wrong ...
         case wrappedMessageBytes:
            byte[] bytes = (byte[]) value;
            try {
               luceneOptions.addFieldToDocument(QueryFacadeImpl.TYPE_FIELD_NAME, messageDescriptor.getFullName(), document);
               new ProtobufParser().parse(new IndexingTagHandler(messageDescriptor, document, luceneOptions), messageDescriptor, bytes);
            } catch (IOException e) {
               throw new CacheException(e);
            }
            break;
         default:
            throw new IllegalStateException();
      }
   }

   @Override
   public void onStartNested(int fieldNumber, String fieldName, Descriptors.Descriptor messageDescriptor) {
      throw new IllegalStateException("No nested message is expected");
   }

   @Override
   public void onEndNested(int fieldNumber, String fieldName, Descriptors.Descriptor messageDescriptor) {
      throw new IllegalStateException("No nested message is expected");
   }

   @Override
   public void onEnd() {
   }
}
