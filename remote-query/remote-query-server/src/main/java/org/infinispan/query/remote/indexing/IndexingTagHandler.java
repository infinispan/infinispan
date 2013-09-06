package org.infinispan.query.remote.indexing;

import com.google.protobuf.Descriptors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.LuceneOptions;
import org.hibernate.search.engine.impl.LuceneOptionsImpl;
import org.hibernate.search.engine.metadata.impl.DocumentFieldMetadata;
import org.infinispan.protostream.TagHandler;
import org.infinispan.query.remote.QueryFacadeImpl;

/**
 * Extracts and indexes all tags (fields) from a protobuf encoded message.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
class IndexingTagHandler implements TagHandler {

   private static final Integer TRUE_INT = 1;
   private static final Integer FALSE_INT = 0;

   private static final LuceneOptions NOT_STORED_NOT_ANALYZED = new LuceneOptionsImpl(
         new DocumentFieldMetadata.Builder(null, Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.NO)
               .boost(1.0F)
               .build());

   private static final LuceneOptions STORED_NOT_ANALYZED = new LuceneOptionsImpl(
         new DocumentFieldMetadata.Builder(null, Store.YES, Field.Index.NOT_ANALYZED, Field.TermVector.NO)
               .boost(1.0F)
               .build());

   private final Document document;
   private final LuceneOptions defaultLuceneOptions;

   private ReadMessageContext messageContext;

   public IndexingTagHandler(Descriptors.Descriptor messageDescriptor, Document document, LuceneOptions defaultLuceneOptions) {
      this.document = document;
      this.defaultLuceneOptions = defaultLuceneOptions;
      this.messageContext = new ReadMessageContext(null, null, messageDescriptor);
   }

   @Override
   public void onStart() {
   }

   @Override
   public void onTag(int fieldNumber, String fieldName, Descriptors.FieldDescriptor.Type type, Descriptors.FieldDescriptor.JavaType javaType, Object tagValue) {
      messageContext.getSeenFields().add(fieldNumber);

      //todo [anistor] unknown fields are not indexed
      if (fieldName != null && isIndexed(fieldNumber)) {
         addFieldToDocument(fieldName, type, tagValue);
      }
   }

   private void addFieldToDocument(String fieldName, Descriptors.FieldDescriptor.Type type, Object value) {
      LuceneOptions luceneOptions = defaultLuceneOptions;
      LuceneOptions numericLuceneOptions = STORED_NOT_ANALYZED;
      if (value == null) {
         value = QueryFacadeImpl.NULL_TOKEN;  //todo [anistor] do we need a specific null token for numeric fields?
         luceneOptions = NOT_STORED_NOT_ANALYZED;
         numericLuceneOptions = NOT_STORED_NOT_ANALYZED;
      }

      String fn = getFullFieldName(fieldName); //todo [anistor] should we index with fieldNumber instead of fieldName?
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
            numericLuceneOptions.addNumericFieldToDocument(fn, value, document);
            break;
         case BOOL:
            numericLuceneOptions.addNumericFieldToDocument(fn, ((Boolean) value) ? TRUE_INT : FALSE_INT, document);
            break;
         default:
            luceneOptions.addFieldToDocument(fn, String.valueOf(value), document);
      }
   }

   private String getFullFieldName(String fieldName) {
      String fieldPrefix = messageContext.getFullFieldName();
      return fieldPrefix != null ? fieldPrefix + "." + fieldName : fieldName;
   }

   private boolean isIndexed(int fieldNumber) {
      Descriptors.FieldDescriptor fd = messageContext.getFieldByNumber(fieldNumber);
      //todo [anistor] right now we index everything. check field Options and see if [(Indexed)] is present
      return true;
   }

   @Override
   public void onStartNested(int fieldNumber, String fieldName, Descriptors.Descriptor messageDescriptor) {
      messageContext.getSeenFields().add(fieldNumber);
      pushContext(fieldName, messageDescriptor);
   }

   @Override
   public void onEndNested(int fieldNumber, String fieldName, Descriptors.Descriptor messageDescriptor) {
      popContext();
   }

   @Override
   public void onEnd() {
      indexMissingFields();
   }

   private void pushContext(String fieldName, Descriptors.Descriptor messageDescriptor) {
      messageContext = new ReadMessageContext(messageContext, fieldName, messageDescriptor);
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
      for (Descriptors.FieldDescriptor fd : messageContext.getMessageDescriptor().getFields()) {
         if (!messageContext.getSeenFields().contains(fd.getNumber())) {
            Object defaultValue = fd.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                  || fd.getType() == Descriptors.FieldDescriptor.Type.GROUP ? null : fd.getDefaultValue();
            addFieldToDocument(fd.getName(), fd.getType(), defaultValue);
         }
      }
   }
}
