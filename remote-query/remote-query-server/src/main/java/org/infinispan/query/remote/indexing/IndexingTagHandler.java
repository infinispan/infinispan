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

   private static final LuceneOptions STORED_NOT_ANALYZED = new LuceneOptionsImpl(
         new DocumentFieldMetadata.Builder(null, Store.YES, Field.Index.NOT_ANALYZED, Field.TermVector.NO)
               .boost(1.0F)
               .build());

   private final Document document;
   private final LuceneOptions defaultLuceneOptions;

   private String fieldPrefix = null;

   private MessageContext messageContext;

   public IndexingTagHandler(Descriptors.Descriptor messageDescriptor, Document document, LuceneOptions defaultLuceneOptions) {
      this.document = document;
      this.defaultLuceneOptions = defaultLuceneOptions;
      this.messageContext = new MessageContext(messageDescriptor);
   }

   @Override
   public void onStart() {
   }

   @Override
   public void onTag(int fieldNumber, String fieldName, Descriptors.FieldDescriptor.Type type, Descriptors.FieldDescriptor.JavaType javaType, Object value) {
      messageContext.getReadFields().add(fieldNumber);

      //todo [anistor] unknown fields are not indexed
      //todo [anistor] should we index with fieldNumber instead of fieldName?
      if (fieldName != null && isIndexed(fieldName)) {
         String fn = getFieldFullName(fieldName);
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
               STORED_NOT_ANALYZED.addNumericFieldToDocument(fn, value, document);
               break;
            case BOOL:
               STORED_NOT_ANALYZED.addNumericFieldToDocument(fn, ((Boolean) value) ? TRUE_INT : FALSE_INT, document);
               break;
            default:
               defaultLuceneOptions.addFieldToDocument(fn, String.valueOf(value), document);
         }
      }
   }

   private String getFieldFullName(String fieldName) {
      return fieldPrefix != null ? fieldPrefix + fieldName : fieldName;
   }

   private boolean isIndexed(String fieldName) {
      Descriptors.FieldDescriptor fd = messageContext.getFieldByName(fieldName);
      //todo [anistor] right now we index everything. check field Options and see if [(Indexed)] is present
      return true;
   }

   @Override
   public void onStartNested(int fieldNumber, String fieldName, Descriptors.Descriptor messageDescriptor) {
      messageContext.getReadFields().add(fieldNumber);
      pushContext(fieldName, messageDescriptor);
   }

   @Override
   public void onEndNested(int fieldNumber, String fieldName, Descriptors.Descriptor messageDescriptor) {
      popContext();
   }

   @Override
   public void onEnd() {
      indexMissingFieldsAsNull();
   }

   private void pushContext(String fieldName, Descriptors.Descriptor messageDescriptor) {
      fieldPrefix = fieldPrefix == null ? fieldName + "." : fieldPrefix + "." + fieldName + ".";
      messageContext = new MessageContext(fieldName, messageContext, messageDescriptor);
   }

   private void popContext() {
      assert fieldPrefix != null;
      int pos = fieldPrefix.length() - 2;
      assert pos >= 0;
      while (pos > 0 && fieldPrefix.charAt(pos) != '.') {
         pos--;
      }

      indexMissingFieldsAsNull();

      fieldPrefix = fieldPrefix.substring(0, pos);
      messageContext = messageContext.getParentContext();
   }

   private void indexMissingFieldsAsNull() {
      for (Descriptors.FieldDescriptor fd : messageContext.getMessageDescriptor().getFields()) {
         if (!messageContext.getReadFields().contains(fd.getNumber())) {
            defaultLuceneOptions.addFieldToDocument(getFieldFullName(fd.getName()), QueryFacadeImpl.NULL_TOKEN, document);
         }
      }
   }
}
