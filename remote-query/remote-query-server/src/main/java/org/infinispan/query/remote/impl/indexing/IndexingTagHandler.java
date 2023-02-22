package org.infinispan.query.remote.impl.indexing;

import org.hibernate.search.engine.backend.document.DocumentElement;
import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.hibernate.search.engine.backend.document.IndexObjectFieldReference;
import org.infinispan.protostream.TagHandler;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.remote.impl.indexing.aggregator.BigDecimalAggregator;
import org.infinispan.query.remote.impl.indexing.aggregator.BigIntegerAggregator;
import org.infinispan.query.remote.impl.indexing.aggregator.TypeAggregator;
import org.infinispan.query.remote.impl.mapping.reference.FieldReferenceProvider;
import org.infinispan.query.remote.impl.mapping.reference.IndexReferenceHolder;

/**
 * Extracts and indexes all tags (fields) from a protobuf encoded message.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class IndexingTagHandler implements TagHandler {

   private final IndexReferenceHolder indexReferenceHolder;

   private IndexingMessageContext messageContext;

   public IndexingTagHandler(Descriptor messageDescriptor, DocumentElement document, IndexReferenceHolder indexReferenceHolder) {
      this.indexReferenceHolder = indexReferenceHolder;
      this.messageContext = new IndexingMessageContext(null, null, messageDescriptor, document, null);
   }

   @Override
   public void onTag(int fieldNumber, FieldDescriptor fieldDescriptor, Object tagValue) {
      messageContext.markField(fieldNumber);

      if (fieldDescriptor == null) {
         // Unknown fields are not indexed.
         return;
      }

      TypeAggregator typeAggregator = messageContext.getTypeAggregator();
      if (typeAggregator != null) {
         typeAggregator.add(fieldDescriptor, tagValue);
         return;
      }

      addFieldToDocument(fieldDescriptor, tagValue);
   }

   private void addFieldToDocument(FieldDescriptor fieldDescriptor, Object value) {
      // We always use fully qualified field names because Lucene does not allow two identically named fields defined by
      // different entity types to have different field types or different indexing options in the same index.
      String fieldPath = messageContext.getFieldPath();
      fieldPath = fieldPath != null ? fieldPath + '.' + fieldDescriptor.getName() : fieldDescriptor.getName();
      IndexFieldReference<?> fieldReference = indexReferenceHolder.getFieldReference(fieldPath);
      if (fieldReference != null) {
         messageContext.addValue(fieldReference, value);
      }
   }

   @Override
   public void onStartNested(int fieldNumber, FieldDescriptor fieldDescriptor) {
      messageContext.markField(fieldNumber);
      pushContext(fieldDescriptor, fieldDescriptor.getMessageType());
   }

   @Override
   public void onEndNested(int fieldNumber, FieldDescriptor fieldDescriptor) {
      popContext();
   }

   @Override
   public void onEnd() {
      indexMissingFields();
   }

   private void pushContext(FieldDescriptor fieldDescriptor, Descriptor messageDescriptor) {
      String fieldPath = messageContext.getFieldPath();
      fieldPath = fieldPath != null ? fieldPath + '.' + fieldDescriptor.getName() : fieldDescriptor.getName();

      IndexFieldReference<?> messageField = indexReferenceHolder.getFieldReference(fieldPath);
      if (messageField != null) {
         if (pushMessageFieldContext(fieldDescriptor, messageDescriptor, messageField)) {
            return;
         }
      }

      DocumentElement documentElement = null;
      if (messageContext.getDocument() != null) {
         IndexObjectFieldReference objectReference = indexReferenceHolder.getObjectReference(fieldPath);
         if (objectReference != null) {
            documentElement = messageContext.getDocument().addObject(objectReference);
         }
      }

      messageContext = new IndexingMessageContext(messageContext, fieldDescriptor, messageDescriptor, documentElement, null);
   }

   private boolean pushMessageFieldContext(FieldDescriptor fieldDescriptor, Descriptor messageDescriptor,
                                           IndexFieldReference<?> messageField) {
      String fullName = messageDescriptor.getFullName();
      if (FieldReferenceProvider.BIG_INTEGER_COMMON_TYPE.equals(fullName)) {
         messageContext = new IndexingMessageContext(messageContext, fieldDescriptor, messageDescriptor,
               messageContext.getDocument(), new BigIntegerAggregator(messageField));
         return true;
      }
      if (FieldReferenceProvider.BIG_DECIMAL_COMMON_TYPE.equals(fullName)) {
         messageContext = new IndexingMessageContext(messageContext, fieldDescriptor, messageDescriptor,
               messageContext.getDocument(), new BigDecimalAggregator(messageField));
         return true;
      }
      return false;
   }

   private void popContext() {
      TypeAggregator typeAggregator = messageContext.getTypeAggregator();
      if (typeAggregator != null) {
         typeAggregator.addValue(messageContext);
      } else {
         indexMissingFields();
      }

      messageContext = messageContext.getParentContext();
   }

   /**
    * All fields that were not seen until the end of this message are missing and will be indexed with their default
    * value or null if none was declared. The null value is replaced with a special null token placeholder because
    * Lucene cannot index nulls.
    */
   private void indexMissingFields() {
      if (messageContext.getDocument() == null) {
         return;
      }

      for (FieldDescriptor fieldDescriptor : messageContext.getMessageDescriptor().getFields()) {
         if (!messageContext.isFieldMarked(fieldDescriptor.getNumber())) {
            Object defaultValue = fieldDescriptor.hasDefaultValue() ? fieldDescriptor.getDefaultValue() : null;
            addFieldToDocument(fieldDescriptor, defaultValue);
         }
      }
   }
}
