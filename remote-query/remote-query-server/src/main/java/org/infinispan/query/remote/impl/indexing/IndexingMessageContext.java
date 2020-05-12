package org.infinispan.query.remote.impl.indexing;

import org.hibernate.search.engine.backend.document.DocumentElement;
import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.infinispan.protostream.MessageContext;
import org.infinispan.protostream.descriptors.Descriptor;

final class IndexingMessageContext extends MessageContext<IndexingMessageContext> {

   // null if the embedded is not indexed
   private final DocumentElement document;

   public IndexingMessageContext(IndexingMessageContext parentContext, String fieldName, Descriptor messageDescriptor, DocumentElement document) {
      super(parentContext, fieldName, messageDescriptor);
      this.document = document;
   }

   public DocumentElement getDocument() {
      return document;
   }

   public void addValue(IndexFieldReference fieldReference, Object value) {
      if (document != null) {
         // using raw type for IndexFieldReference
         // value type and fieldReference value type are supposed to match
         document.addValue(fieldReference, value);
      }
   }
}
