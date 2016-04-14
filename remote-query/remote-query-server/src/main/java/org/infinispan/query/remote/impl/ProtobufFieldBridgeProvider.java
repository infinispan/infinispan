package org.infinispan.query.remote.impl;

import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.builtin.BooleanBridge;
import org.hibernate.search.bridge.builtin.NumericFieldBridge;
import org.hibernate.search.bridge.builtin.StringBridge;
import org.hibernate.search.bridge.builtin.impl.NullEncodingTwoWayFieldBridge;
import org.hibernate.search.bridge.builtin.impl.TwoWayString2FieldBridgeAdaptor;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.dsl.embedded.impl.jpalucene.LuceneQueryMaker;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;
import org.infinispan.query.remote.impl.logging.Log;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
final class ProtobufFieldBridgeProvider implements LuceneQueryMaker.FieldBridgeProvider {

   private static final Log log = LogFactory.getLog(ProtobufFieldBridgeProvider.class, Log.class);

   private static final FieldBridge DOUBLE_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.DOUBLE_FIELD_BRIDGE, QueryFacadeImpl.NULL_TOKEN_CODEC);

   private static final FieldBridge FLOAT_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.FLOAT_FIELD_BRIDGE, QueryFacadeImpl.NULL_TOKEN_CODEC);

   private static final FieldBridge LONG_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.LONG_FIELD_BRIDGE, QueryFacadeImpl.NULL_TOKEN_CODEC);

   private static final FieldBridge INT_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.INT_FIELD_BRIDGE, QueryFacadeImpl.NULL_TOKEN_CODEC);

   private static final FieldBridge STRING_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(StringBridge.INSTANCE), QueryFacadeImpl.NULL_TOKEN_CODEC);

   private static final FieldBridge BOOL_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(new BooleanBridge()), QueryFacadeImpl.NULL_TOKEN_CODEC);

   private final SerializationContext serializationContext;

   ProtobufFieldBridgeProvider(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   @Override
   public FieldBridge getFieldBridge(String typeName, String[] propertyPath) {
      FieldDescriptor fd = getFieldDescriptor(typeName, propertyPath);
      switch (fd.getType()) {
         case DOUBLE:
            return DOUBLE_FIELD_BRIDGE;
         case FLOAT:
            return FLOAT_FIELD_BRIDGE;
         case INT64:
         case UINT64:
         case FIXED64:
         case SFIXED64:
         case SINT64:
            return LONG_FIELD_BRIDGE;
         case INT32:
         case FIXED32:
         case UINT32:
         case SFIXED32:
         case SINT32:
         case ENUM:
            return INT_FIELD_BRIDGE;
         case BOOL:
            return BOOL_FIELD_BRIDGE;
         case STRING:
         case BYTES:
         case GROUP:
         case MESSAGE:
            return STRING_FIELD_BRIDGE;
      }
      return null;
   }

   private FieldDescriptor getFieldDescriptor(String typeName, String[] propertyPath) {
      Descriptor messageDescriptor = serializationContext.getMessageDescriptor(typeName);
      FieldDescriptor fd = null;
      for (int i = 0; i < propertyPath.length; i++) {
         String name = propertyPath[i];
         fd = messageDescriptor.findFieldByName(name);
         if (fd == null) {
            throw log.unknownField(name, messageDescriptor.getFullName());
         }
         IndexingMetadata indexingMetadata = messageDescriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         if (indexingMetadata != null && !indexingMetadata.isFieldIndexed(fd.getNumber())) {
            throw log.fieldIsNotIndexed(name, messageDescriptor.getFullName());
         }
         if (i < propertyPath.length - 1) {
            messageDescriptor = fd.getMessageType();
         }
      }
      return fd;
   }
}
