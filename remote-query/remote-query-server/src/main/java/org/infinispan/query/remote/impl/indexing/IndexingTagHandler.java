package org.infinispan.query.remote.impl.indexing;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.LuceneOptions;
import org.hibernate.search.engine.impl.LuceneOptionsImpl;
import org.hibernate.search.engine.metadata.impl.BackReference;
import org.hibernate.search.engine.metadata.impl.DocumentFieldMetadata;
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

   private static final LuceneOptions NOT_STORED_NOT_ANALYZED = new LuceneOptionsImpl(
         new DocumentFieldMetadata.Builder(new BackReference<>(), new BackReference<>(), null, Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.NO)
               .indexNullAs(QueryFacadeImpl.NULL_TOKEN_CODEC)
               .boost(1.0F)
               .build(), 1.0F, 1.0F);

   private static final LuceneOptions STORED_NOT_ANALYZED = new LuceneOptionsImpl(
         new DocumentFieldMetadata.Builder(new BackReference<>(), new BackReference<>(), null, Store.YES, Field.Index.NOT_ANALYZED, Field.TermVector.NO)
               .indexNullAs(QueryFacadeImpl.NULL_TOKEN_CODEC)
               .boost(1.0F)
               .build(), 1.0F, 1.0F);

   private final Document document;

   private MessageContext<MessageContext> messageContext;

   public IndexingTagHandler(Descriptor messageDescriptor, Document document) {
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
            addFieldToDocument(fieldDescriptor.getName(), fieldDescriptor.getType(), tagValue, fieldMapping);
         }
      }
   }

   private void addFieldToDocument(String fieldName, Type type, Object value, FieldMapping fieldMapping) {
      LuceneOptions luceneOptions;
      if (fieldMapping == null) {
         // This comes from a message definition that does not have any annotations and is treated as if
         // everything is indexed, stored, and not analyzed, for compatibility reasons with first version of remote query.
         // TODO [anistor] this behaviour is deprecated and will be removed in Infinispan 10.0
         luceneOptions = STORED_NOT_ANALYZED;
         if (value == null) {
            value = QueryFacadeImpl.NULL_TOKEN;  //todo [anistor] do we need a specific null token for numeric fields? we also need a way to specify the null token in the schema instead of assuming it.
            type = Type.STRING;
            luceneOptions = NOT_STORED_NOT_ANALYZED;
         }
      } else {
         luceneOptions = fieldMapping.luceneOptions();
         if (value == null) {
            if (fieldMapping.analyze()) {
               // a missing or null field will not get indexed as the 'null token' if it is analyzed
               return;
            }
            value = QueryFacadeImpl.NULL_TOKEN;  //todo [anistor] do we need a specific null token for numeric fields? we also need a way to specify the null token in the schema instead of assuming it.
            type = Type.STRING;
            luceneOptions = NOT_STORED_NOT_ANALYZED;
         }
      }

      // We always use fully qualified field names because Lucene does not allow two identically named fields defined by
      // different entity types to have different field types or different indexing options.
      String fn = getFullFieldName(fieldName);
      //todo [anistor] string vs numeric. use a proper way to transform to string
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
            if (!value.equals(QueryFacadeImpl.NULL_TOKEN)) {
               luceneOptions.addNumericFieldToDocument(fn, value, document);
            }
            break;
         case BOOL:
            luceneOptions.addFieldToDocument(fn, value.toString(), document);
            break;
         default:
            luceneOptions.addFieldToDocument(fn, String.valueOf(value), document);
      }
   }

   private String getFullFieldName(String fieldName) {
      String fieldPrefix = messageContext.getFullFieldName();
      return fieldPrefix != null ? fieldPrefix + "." + fieldName : fieldName;
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
      for (FieldDescriptor fd : messageContext.getMessageDescriptor().getFields()) {
         if (!messageContext.isFieldMarked(fd.getNumber())) {
            Object defaultValue = fd.getJavaType() == JavaType.MESSAGE
                  || fd.hasDefaultValue() ? fd.getDefaultValue() : null;
            IndexingMetadata indexingMetadata = messageContext.getMessageDescriptor().getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
            FieldMapping fieldMapping = indexingMetadata != null ? indexingMetadata.getFieldMapping(fd.getName()) : null;
            if (indexingMetadata == null || fieldMapping != null && fieldMapping.index()) {
               addFieldToDocument(fd.getName(), fd.getType(), defaultValue, fieldMapping);
            }
         }
      }
   }
}
