package org.infinispan.query.remote.impl.indexing;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.LuceneOptions;
import org.hibernate.search.bridge.util.impl.ToStringNullMarker;
import org.hibernate.search.engine.impl.LuceneOptionsImpl;
import org.hibernate.search.engine.metadata.impl.BackReference;
import org.hibernate.search.engine.metadata.impl.DocumentFieldMetadata;
import org.hibernate.search.engine.nulls.codec.impl.LuceneStringNullMarkerCodec;
import org.hibernate.search.engine.nulls.codec.impl.NullMarkerCodec;
import org.infinispan.protostream.MessageContext;
import org.infinispan.protostream.TagHandler;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.JavaType;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.query.remote.impl.QueryFacadeImpl;

/**
 * Extracts and indexes all tags (fields) from a protobuf encoded message.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
final class IndexingTagHandler implements TagHandler {

   private static final NullMarkerCodec NULL_TOKEN_CODEC = new LuceneStringNullMarkerCodec(new ToStringNullMarker(IndexingMetadata.DEFAULT_NULL_TOKEN));

   private static final LuceneOptions NOT_STORED_NOT_ANALYZED = new LuceneOptionsImpl(
         new DocumentFieldMetadata.Builder(BackReference.empty(), BackReference.empty(), null, null, Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.NO)
               .indexNullAs(NULL_TOKEN_CODEC)
               .boost(1.0F)
               .build(), 1.0F, 1.0F);

   private static final LuceneOptions STORED_NOT_ANALYZED = new LuceneOptionsImpl(
         new DocumentFieldMetadata.Builder(BackReference.empty(), BackReference.empty(), null, null, Store.YES, Field.Index.NOT_ANALYZED, Field.TermVector.NO)
               .indexNullAs(NULL_TOKEN_CODEC)
               .boost(1.0F)
               .build(), 1.0F, 1.0F);

   private final Document document;

   private MessageContext<? extends MessageContext> messageContext;

   IndexingTagHandler(Descriptor messageDescriptor, Document document) {
      this.document = document;
      this.messageContext = new MessageContext<>(null, null, messageDescriptor);
   }

   @Override
   public void onStart() {
      // add the type discriminator field
      NOT_STORED_NOT_ANALYZED.addFieldToDocument(QueryFacadeImpl.TYPE_FIELD_NAME, messageContext.getMessageDescriptor().getFullName(), document);
   }

   @Override
   public void onTag(int fieldNumber, FieldDescriptor fieldDescriptor, Object tagValue) {
      messageContext.markField(fieldNumber);

      // Unknown fields are not indexed.
      if (fieldDescriptor != null) {
         IndexingMetadata indexingMetadata = messageContext.getMessageDescriptor().getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         FieldMapping fieldMapping = indexingMetadata != null ? indexingMetadata.getFieldMapping(fieldDescriptor.getName()) : null;
         if (indexingMetadata == null || fieldMapping != null && fieldMapping.index()) {
            //TODO [anistor] should we still store if isStore==true but isIndexed==false?
            addFieldToDocument(fieldDescriptor, tagValue, fieldMapping);
         }
      }
   }

   private void addFieldToDocument(FieldDescriptor fieldDescriptor, Object value, FieldMapping fieldMapping) {
      Type type = fieldDescriptor.getType();
      LuceneOptions luceneOptions;
      boolean isSortable = false;
      if (fieldMapping == null) {
         // TODO [anistor] this behaviour is deprecated and will be removed in Infinispan 10.0
         // WE DO NOT HAVE A FIELD MAPPING!
         // This comes from a message definition that does not have any annotations and is treated as if
         // everything is indexed, stored, and not analyzed for compatibility reasons with first version of remote query.
         // All null values (regardless of type) are indexed as a string as if indexNullAs == "_null_" (see IndexingMetadata.DEFAULT_NULL_TOKEN)
         if (value == null) {
            value = IndexingMetadata.DEFAULT_NULL_TOKEN;
            type = Type.STRING;  // we add a string to the index even if the field is numeric!
            luceneOptions = NOT_STORED_NOT_ANALYZED;
         } else {
            luceneOptions = STORED_NOT_ANALYZED;
         }
      } else {
         luceneOptions = fieldMapping.luceneOptions();
         isSortable = fieldMapping.sortable();
         if (value == null) {
            if (fieldMapping.indexNullAs() == null || fieldMapping.analyze()) {
               // a missing or null field will never get indexed as the 'null token' if it is analyzed
               return;
            }
            value = fieldMapping.indexNullAs();
         }
      }

      // We always use fully qualified field names because Lucene does not allow two identically named fields defined by
      // different entity types to have different field types or different indexing options in the same index.
      String fullFieldName = messageContext.getFullFieldName();
      fullFieldName = fullFieldName != null ? fullFieldName + "." + fieldDescriptor.getName() : fieldDescriptor.getName();
      switch (type) {
         case DOUBLE:
         case FLOAT:
         case INT64:
         case UINT64:
         case INT32:
         case FIXED64:
         case FIXED32:
         case UINT32:
         case SFIXED32:
         case SFIXED64:
         case SINT32:
         case SINT64:
         case ENUM:
            if (isSortable) {
               luceneOptions.addNumericDocValuesFieldToDocument(fullFieldName, (Number) value, document);
            }
            luceneOptions.addNumericFieldToDocument(fullFieldName, value, document);
            break;
         default:
            String indexedString = String.valueOf(value);
            if (isSortable) {
               luceneOptions.addSortedDocValuesFieldToDocument(fullFieldName, indexedString, document);
            }
            luceneOptions.addFieldToDocument(fullFieldName, indexedString, document);
      }
   }

   @Override
   public void onStartNested(int fieldNumber, FieldDescriptor fieldDescriptor) {
      messageContext.markField(fieldNumber);
      pushContext(fieldDescriptor.getName(), fieldDescriptor.getMessageType());
   }

   @Override
   public void onEndNested(int fieldNumber, FieldDescriptor fieldDescriptor) {
      popContext();
   }

   @Override
   public void onEnd() {
      indexMissingFields();
   }

   private void pushContext(String fieldName, Descriptor messageDescriptor) {
      messageContext = new MessageContext<>(messageContext, fieldName, messageDescriptor);
   }

   private void popContext() {
      indexMissingFields();
      messageContext = messageContext.getParentContext();
   }

   /**
    * All fields that were not seen until the end of this message are missing and will be indexed with their default
    * value or null if none was declared. The null value is replaced with a special null token placeholder because
    * Lucene cannot index nulls.
    */
   private void indexMissingFields() {
      for (FieldDescriptor fieldDescriptor : messageContext.getMessageDescriptor().getFields()) {
         if (!messageContext.isFieldMarked(fieldDescriptor.getNumber())) {
            Object defaultValue = fieldDescriptor.getJavaType() == JavaType.MESSAGE
                  || fieldDescriptor.hasDefaultValue() ? fieldDescriptor.getDefaultValue() : null;
            IndexingMetadata indexingMetadata = messageContext.getMessageDescriptor().getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
            FieldMapping fieldMapping = indexingMetadata != null ? indexingMetadata.getFieldMapping(fieldDescriptor.getName()) : null;
            if (indexingMetadata == null || fieldMapping != null && fieldMapping.index()) {
               addFieldToDocument(fieldDescriptor, defaultValue, fieldMapping);
            }
         }
      }
   }
}
