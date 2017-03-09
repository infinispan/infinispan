package org.infinispan.query.remote.impl;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.LuceneOptions;
import org.hibernate.search.bridge.builtin.BooleanBridge;
import org.hibernate.search.bridge.builtin.NumericFieldBridge;
import org.hibernate.search.bridge.builtin.StringBridge;
import org.hibernate.search.bridge.builtin.impl.NullEncodingTwoWayFieldBridge;
import org.hibernate.search.bridge.spi.NullMarker;
import org.hibernate.search.bridge.util.impl.ToStringNullMarker;
import org.hibernate.search.bridge.util.impl.TwoWayString2FieldBridgeAdaptor;
import org.hibernate.search.engine.nulls.codec.impl.LuceneIntegerNullMarkerCodec;
import org.hibernate.search.engine.nulls.codec.impl.LuceneLongNullMarkerCodec;
import org.hibernate.search.engine.nulls.codec.impl.LuceneStringNullMarkerCodec;
import org.hibernate.search.engine.nulls.codec.impl.NullMarkerCodec;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.query.dsl.embedded.impl.LuceneQueryMaker;
import org.infinispan.query.remote.impl.indexing.FieldMapping;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;
import org.infinispan.query.remote.impl.logging.Log;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
final class ProtobufFieldBridgeProvider implements LuceneQueryMaker.FieldBridgeProvider<Descriptor> {

   private static final Log log = LogFactory.getLog(ProtobufFieldBridgeProvider.class, Log.class);

   private static final LuceneStringNullMarkerCodec LUCENE_STRING_NULL_MARKER_CODEC = new LuceneStringNullMarkerCodec(new ToStringNullMarker(IndexingMetadata.DEFAULT_NULL_TOKEN));

   private static final FieldBridge DOUBLE_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.DOUBLE_FIELD_BRIDGE, LUCENE_STRING_NULL_MARKER_CODEC);

   private static final FieldBridge FLOAT_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.FLOAT_FIELD_BRIDGE, LUCENE_STRING_NULL_MARKER_CODEC);

   private static final FieldBridge LONG_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.LONG_FIELD_BRIDGE, LUCENE_STRING_NULL_MARKER_CODEC);

   private static final FieldBridge INT_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(NumericFieldBridge.INT_FIELD_BRIDGE, LUCENE_STRING_NULL_MARKER_CODEC);

   private static final FieldBridge STRING_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(StringBridge.INSTANCE), LUCENE_STRING_NULL_MARKER_CODEC);

   private static final FieldBridge BOOL_FIELD_BRIDGE = new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(new BooleanBridge()), LUCENE_STRING_NULL_MARKER_CODEC);

   private static final NullMarkerCodec NOT_ENCODING_NULL = new NullMarkerCodec() {

      @Override
      public NullMarker getNullMarker() {
         return null;
      }

      @Override
      public void encodeNullValue(String fieldName, Document document, LuceneOptions luceneOptions) {
      }

      @Override
      public Query createNullMatchingQuery(String fieldName) {
         throw new IllegalStateException("Cannot build IS NULL query for field '" + fieldName + "' which does not have indexNullAs configured for indexing null values.");
      }

      @Override
      public boolean representsNullValue(IndexableField field) {
         return field == null;
      }
   };

   ProtobufFieldBridgeProvider() {
   }

   @Override
   public FieldBridge getFieldBridge(Descriptor typeMetadata, String[] propertyPath) {
      FieldDescriptor fd = getFieldDescriptor(typeMetadata, propertyPath);
      IndexingMetadata indexingMetadata = fd.getContainingMessage().getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
      FieldMapping fieldMapping = indexingMetadata != null ? indexingMetadata.getFieldMapping(fd.getName()) : null;

      if (fieldMapping != null) {
         FieldBridge fieldBridge = fieldMapping.getFieldBridge();
         if (fieldBridge == null) {
            fieldBridge = makeFieldBridge(fd, fieldMapping.indexNullAs());
            fieldMapping.setFieldBridge(fieldBridge);
         }
         return fieldBridge;
      }

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

   private FieldBridge makeFieldBridge(FieldDescriptor fd, String indexNullAs) {
      if (indexNullAs == null) {
         throw new IllegalArgumentException("indexNullAs argument must not be null");
      }
      Type type = fd.getType();
      if (type == null) {
         throw new IllegalStateException("FieldDescriptors are not fully initialised!");
      }
      switch (type) {
         case DOUBLE:
            return indexNullAs.equals(IndexingMetadata.DO_NOT_INDEX_NULL) ?
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.DOUBLE_FIELD_BRIDGE, NOT_ENCODING_NULL) :
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.DOUBLE_FIELD_BRIDGE, new LuceneLongNullMarkerCodec(new ToStringNullMarker(Double.parseDouble(indexNullAs))));
         case FLOAT:
            return indexNullAs.equals(IndexingMetadata.DO_NOT_INDEX_NULL) ?
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.FLOAT_FIELD_BRIDGE, NOT_ENCODING_NULL) :
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.FLOAT_FIELD_BRIDGE, new LuceneLongNullMarkerCodec(new ToStringNullMarker(Float.parseFloat(indexNullAs))));
         case INT64:
         case UINT64:
         case FIXED64:
         case SFIXED64:
         case SINT64:
            return indexNullAs.equals(IndexingMetadata.DO_NOT_INDEX_NULL) ?
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.LONG_FIELD_BRIDGE, NOT_ENCODING_NULL) :
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.LONG_FIELD_BRIDGE, new LuceneLongNullMarkerCodec(new ToStringNullMarker(Long.parseLong(indexNullAs))));
         case INT32:
         case FIXED32:
         case UINT32:
         case SFIXED32:
         case SINT32:
         case ENUM:
            return indexNullAs.equals(IndexingMetadata.DO_NOT_INDEX_NULL) ?
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.INT_FIELD_BRIDGE, NOT_ENCODING_NULL) :
                  new NullEncodingTwoWayFieldBridge(NumericFieldBridge.INT_FIELD_BRIDGE, new LuceneIntegerNullMarkerCodec(new ToStringNullMarker(Integer.parseInt(indexNullAs))));
         case BOOL:
            return indexNullAs.equals(IndexingMetadata.DO_NOT_INDEX_NULL) ?
                  new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(new BooleanBridge()), NOT_ENCODING_NULL) :
                  new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(new BooleanBridge()), new LuceneStringNullMarkerCodec(new ToStringNullMarker(indexNullAs)));
         default:
            return indexNullAs.equals(IndexingMetadata.DO_NOT_INDEX_NULL) ?
                  new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(StringBridge.INSTANCE), NOT_ENCODING_NULL) :
                  new NullEncodingTwoWayFieldBridge(new TwoWayString2FieldBridgeAdaptor(StringBridge.INSTANCE), new LuceneStringNullMarkerCodec(new ToStringNullMarker(indexNullAs)));
      }
   }

   private FieldDescriptor getFieldDescriptor(Descriptor typeMetadata, String[] propertyPath) {
      Descriptor messageDescriptor = typeMetadata;
      FieldDescriptor fd = null;
      for (int i = 0; i < propertyPath.length; i++) {
         String name = propertyPath[i];
         fd = messageDescriptor.findFieldByName(name);
         if (fd == null) {
            throw log.unknownField(name, messageDescriptor.getFullName());
         }
         IndexingMetadata indexingMetadata = messageDescriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         if (indexingMetadata != null && !indexingMetadata.isFieldIndexed(fd.getName())) {
            throw log.fieldIsNotIndexed(name, messageDescriptor.getFullName());
         }
         if (i < propertyPath.length - 1) {
            messageDescriptor = fd.getMessageType();
         }
      }
      return fd;
   }
}
