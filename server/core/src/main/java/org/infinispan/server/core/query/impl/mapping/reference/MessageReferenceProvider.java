package org.infinispan.server.core.query.impl.mapping.reference;

import static org.infinispan.server.core.query.impl.indexing.IndexingMetadata.findProcessedAnnotation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.hibernate.search.engine.backend.document.model.dsl.IndexSchemaElement;
import org.hibernate.search.engine.backend.types.ObjectStructure;
import org.hibernate.search.engine.spatial.GeoPoint;
import org.infinispan.api.annotations.indexing.option.Structure;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.server.core.query.impl.indexing.FieldMapping;
import org.infinispan.server.core.query.impl.indexing.IndexingKeyMetadata;
import org.infinispan.server.core.query.impl.indexing.IndexingMetadata;
import org.infinispan.server.core.query.impl.indexing.SpatialFieldMapping;
import org.infinispan.server.core.query.impl.indexing.infinispan.IndexingMetadataHolder;

/**
 * Provides indexing information about a {@link Descriptor}.
 *
 * @since 12.0
 */
public final class MessageReferenceProvider {

   public static final Set<String> COMMON_MESSAGE_TYPES =
         new HashSet<>(Arrays.asList(FieldReferenceProvider.COMMON_MESSAGE_TYPES));

   private final IndexingMetadata indexingMetadata;
   private final List<FieldReferenceProvider> fields;
   private final List<SpatialReferenceProvider> geoFields;
   private final List<Embedded> embedded;
   private final String keyMessageName;
   private final String keyPropertyName;

   public MessageReferenceProvider(Descriptor descriptor) {
      this(descriptor, findProcessedAnnotation(descriptor, IndexingMetadata.INDEXED_ANNOTATION));
   }

   public MessageReferenceProvider(Descriptor descriptor, IndexingMetadata indexingMetadata) {
      this.indexingMetadata = indexingMetadata;
      this.fields = new ArrayList<>(descriptor.getFields().size());
      this.embedded = new ArrayList<>();
      // Skip if not annotated with @Indexed
      if (indexingMetadata == null) {
         keyMessageName = null;
         keyPropertyName = null;
         geoFields = null;
         return;
      }

      this.geoFields = new ArrayList<>(indexingMetadata.getSpatialFields().size());

      for (FieldDescriptor fieldDescriptor : descriptor.getFields()) {
         String fieldName = fieldDescriptor.getName();
         FieldMapping fieldMapping = indexingMetadata.getFieldMapping(fieldName);
         if (fieldMapping == null) {
            // field model is not mapped
            continue;
         }

         if (Type.MESSAGE.equals(fieldDescriptor.getType()) &&
               !COMMON_MESSAGE_TYPES.contains(fieldDescriptor.getTypeName())) {
            // To map the embedded we only require that the embedded field is searchable,
            // embedded types non-root-indexed are also allowed. (see ISPN-16314)
            if (fieldMapping.searchable()) {
               // Hibernate Search can handle the @Field regardless of its attributes
               embedded.add(new Embedded(fieldName, fieldDescriptor.getMessageType().getFullName(),
                     fieldDescriptor.isRepeated(), fieldMapping, fieldDescriptor.getProcessedAnnotation("Embedded")));
            }
            continue;
         }

         FieldReferenceProvider fieldReferenceProvider = new FieldReferenceProvider(fieldDescriptor, fieldMapping);
         if (!fieldReferenceProvider.nothingToBind()) {
            this.fields.add(fieldReferenceProvider);
         }
      }

      IndexingKeyMetadata keyMetadata = indexingMetadata.indexingKey();
      if (keyMetadata != null) {
         embedded.add(new Embedded(keyMetadata.fieldName(), keyMetadata.typeFullName(), keyMetadata.includeDepth()));
         keyMessageName = keyMetadata.typeFullName();
         keyPropertyName = keyMetadata.fieldName();
      } else {
         keyMessageName = null;
         keyPropertyName = null;
      }

      for (SpatialFieldMapping field : indexingMetadata.getSpatialFields().values()) {
         geoFields.add(new SpatialReferenceProvider(field));
      }
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

   public Map<String, IndexReferenceHolder.GeoIndexFieldReference> bindGeo(IndexSchemaElement indexSchemaElement, String basePath) {
      Map<String, IndexReferenceHolder.GeoIndexFieldReference> result = new HashMap<>();
      for (SpatialReferenceProvider field : geoFields) {
         IndexFieldReference<GeoPoint> fieldReference = field.bind(indexSchemaElement);

         String latitudePath = ("".equals(basePath)) ? field.latitudeName() : basePath + "." + field.latitudeName();
         String longitudePath = ("".equals(basePath)) ? field.longitudeName() : basePath + "." + field.longitudeName();
         IndexReferenceHolder.GeoIndexFieldReference latitudeRef =
               new IndexReferenceHolder.GeoIndexFieldReference(IndexReferenceHolder.GeoIndexFieldReference.Role.LAT,
                     fieldReference, field.indexName());
         IndexReferenceHolder.GeoIndexFieldReference longitudeRef =
               new IndexReferenceHolder.GeoIndexFieldReference(IndexReferenceHolder.GeoIndexFieldReference.Role.LON,
                     fieldReference, field.indexName());
         result.put(latitudePath, latitudeRef);
         result.put(longitudePath, longitudeRef);
      }
      return result;
   }

   public List<Embedded> getEmbedded() {
      return embedded;
   }

   public String keyMessageName() {
      return keyMessageName;
   }

   public String keyPropertyName() {
      return keyPropertyName;
   }

   public IndexingMetadata indexingMetadata() {
      return indexingMetadata;
   }

   public static final class Embedded {
      private final String fieldName;
      private final String typeFullName;
      private final boolean repeated;
      private final Integer includeDepth;
      private final ObjectStructure structure;
      private final IndexingMetadataHolder holder;

      private Embedded(String fieldName, String typeFullName, boolean repeated, FieldMapping fieldMapping,
                      IndexingMetadataHolder holder) {
         this.fieldName = fieldName;
         this.typeFullName = typeFullName;
         this.repeated = repeated;
         this.includeDepth = fieldMapping.includeDepth();
         this.structure = (fieldMapping.structure() == null) ? null:
               (Structure.NESTED.equals(fieldMapping.structure())) ? ObjectStructure.NESTED : ObjectStructure.FLATTENED;
         this.holder = holder;
      }

      // typically invoked to create an index-embedded for the cache key
      private Embedded(String fieldName, String typeFullName, Integer includeDepth) {
         this.fieldName = fieldName;
         this.typeFullName = typeFullName;
         this.repeated = false;
         this.includeDepth = includeDepth;
         this.structure = ObjectStructure.DEFAULT; // use the Hibernate Search Lucene backend value
         this.holder = null;
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

      public Integer getIncludeDepth() {
         return includeDepth;
      }

      public ObjectStructure getStructure() {
         return structure;
      }

      public void indexingMetadata(IndexingMetadata indexingMetadata) {
         if (holder != null) {
            holder.setIndexingMetadata(indexingMetadata);
         }
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
