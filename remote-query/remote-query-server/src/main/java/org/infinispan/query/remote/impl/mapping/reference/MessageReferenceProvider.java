package org.infinispan.query.remote.impl.mapping.reference;

import static org.infinispan.query.remote.impl.indexing.IndexingMetadata.findProcessedAnnotation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.hibernate.search.engine.backend.document.model.dsl.IndexSchemaElement;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.query.remote.impl.indexing.FieldMapping;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;

/**
 * Provides indexing information about a {@link Descriptor}.
 *
 * @since 12.0
 */
public class MessageReferenceProvider {

   private final List<FieldReferenceProvider> fields;
   private final List<Embedded> embedded;

   public MessageReferenceProvider(Descriptor descriptor) {
      this.fields = new ArrayList<>(descriptor.getFields().size());
      this.embedded = new ArrayList<>();

      IndexingMetadata indexingMetadata = findProcessedAnnotation(descriptor, IndexingMetadata.INDEXED_ANNOTATION);
      // Skip if not annotated with @Indexed
      if (indexingMetadata == null) {
         return;
      }

      for (FieldDescriptor fieldDescriptor : descriptor.getFields()) {
         String fieldName = fieldDescriptor.getName();
         FieldMapping fieldMapping = indexingMetadata.getFieldMapping(fieldName);
         if (fieldMapping == null) {
            // field model is not mapped
            continue;
         }

         if (Type.MESSAGE.equals(fieldDescriptor.getType())) {
            // If a protobuf field is a Message reference, only take it into account
            // if the Message is @Indexed and has at least one @Field annotation
            if (fieldMapping.index() && isIndexable(fieldDescriptor.getMessageType())) {
               // Hibernate Search can handle the @Field regardless of its attributes
               embedded.add(new Embedded(fieldName, fieldDescriptor.getMessageType().getFullName(), fieldDescriptor.isRepeated()));
            }
            continue;
         }

         FieldReferenceProvider fieldReferenceProvider = new FieldReferenceProvider(fieldDescriptor, fieldMapping);
         if (!fieldReferenceProvider.nothingToBind()) {
            fields.add(fieldReferenceProvider);
         }
      }
   }

   /**
    * Checks if a Descriptors has the @Indexed annotation and at least one @Field
    */
   private boolean isIndexable(Descriptor descriptor) {
      IndexingMetadata indexingMetadata = findProcessedAnnotation(descriptor, IndexingMetadata.INDEXED_ANNOTATION);
      if ( indexingMetadata == null ) {
         return false;
      }

      for (FieldDescriptor fieldDescriptor : descriptor.getFields()) {
         FieldMapping fieldMapping = indexingMetadata.getFieldMapping(fieldDescriptor.getName());
         if (fieldMapping != null) {
            return true;
         }
      }
      return false;
   }

   public boolean isEmpty() {
      return fields.isEmpty();
   }

   public HashMap<String, IndexFieldReference<?>> bind(IndexSchemaElement indexSchemaElement, String basePath) {
      HashMap<String, IndexFieldReference<?>> result = new HashMap<>();
      for (FieldReferenceProvider field : fields) {
         String newPath = ("".equals(basePath)) ? field.getName() : basePath + "." + field.getName();
         result.put(newPath, field.bind(indexSchemaElement));
      }
      return result;
   }

   public List<Embedded> getEmbedded() {
      return embedded;
   }

   public static class Embedded {
      private final String fieldName;
      private final String typeFullName;
      private final boolean repeated;

      public Embedded(String fieldName, String typeFullName, boolean repeated) {
         this.fieldName = fieldName;
         this.typeFullName = typeFullName;
         this.repeated = repeated;
      }

      public String getFieldName() {
         return fieldName;
      }

      public String getTypeFullName() {
         return typeFullName;
      }

      public boolean isRepeated() {
         return repeated;
      }

      @Override
      public String toString() {
         return "{" +
               "fieldName='" + fieldName + '\'' +
               ", typeName='" + typeFullName + '\'' +
               '}';
      }
   }

   @Override
   public String toString() {
      return "{" +
            "fields=" + fields +
            ", embedded=" + embedded +
            '}';
   }
}
