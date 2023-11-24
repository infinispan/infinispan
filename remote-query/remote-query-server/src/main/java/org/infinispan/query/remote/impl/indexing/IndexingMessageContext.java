package org.infinispan.query.remote.impl.indexing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.search.engine.backend.document.DocumentElement;
import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.infinispan.protostream.MessageContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.remote.impl.indexing.aggregator.TypeAggregator;
import org.infinispan.query.remote.impl.mapping.reference.IndexReferenceHolder;

public final class IndexingMessageContext extends MessageContext<IndexingMessageContext> {

   // null if the embedded is not indexed
   private final DocumentElement document;
   private final TypeAggregator typeAggregator;

   private Map<String, List<Float>> vectorAggregators;

   public IndexingMessageContext(IndexingMessageContext parentContext, FieldDescriptor fieldDescriptor,
                                 Descriptor messageDescriptor, DocumentElement document,
                                 TypeAggregator typeAggregator) {
      super(parentContext, fieldDescriptor, messageDescriptor);
      this.document = document;
      this.typeAggregator = typeAggregator;
   }

   public DocumentElement getDocument() {
      return document;
   }

   public TypeAggregator getTypeAggregator() {
      return typeAggregator;
   }

   public void addValue(IndexFieldReference fieldReference, Object value) {
      if (document != null) {
         // using raw type for IndexFieldReference
         // value type and fieldReference value type are supposed to match
         document.addValue(fieldReference, value);
      }
   }

   public void addArrayItem(String fieldPath, Float value) {
      if (vectorAggregators == null) {
         vectorAggregators = new HashMap<>();
      }
      vectorAggregators.putIfAbsent(fieldPath, new ArrayList<>(50)); // guess a quite large value
      vectorAggregators.get(fieldPath).add(value);
   }

   public void writeVectorAggregators(IndexReferenceHolder indexReferenceHolder) {
      if (vectorAggregators == null) {
         return;
      }
      for (Map.Entry<String, List<Float>> entry : vectorAggregators.entrySet()) {
         IndexFieldReference<?> fieldReference = indexReferenceHolder.getFieldReference(entry.getKey());
         List<Float> values = entry.getValue();
         float[] value = new float[values.size()];
         for (int i=0; i<values.size(); i++) {
            value[i] = values.get(i);
         }
         addValue(fieldReference, value);
      }
   }
}
