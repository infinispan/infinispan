package org.infinispan.server.core.query.impl.indexing.search5;

import static org.infinispan.server.core.query.impl.indexing.IndexingMetadata.findAnnotation;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.infinispan.protostream.AnnotationMetadataCreator;
import org.infinispan.protostream.AnnotationParserException;
import org.infinispan.protostream.descriptors.AnnotationElement;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.server.core.query.impl.indexing.FieldMapping;
import org.infinispan.server.core.query.impl.indexing.IndexingKeyMetadata;
import org.infinispan.server.core.query.impl.indexing.IndexingMetadata;
import org.infinispan.server.core.query.impl.indexing.SpatialFieldMapping;
import org.infinispan.server.core.query.impl.indexing.infinispan.InfinispanAnnotations;
import org.infinispan.server.core.query.impl.indexing.infinispan.InfinispanMetadataCreator;
import org.infinispan.server.core.query.impl.logging.Log;
import org.infinispan.query.mapper.mapping.impl.DefaultAnalysisConfigurer;

//todo [anistor] Should be able to have multiple mappings per field like in Hibernate Search, ie. have a @Fields plural annotation

/**
 * {@link AnnotationMetadataCreator} for {@code @Indexed} ProtoStream annotation placed at message type level. Also
 * handles {@code @Field} and {@code @SortableField} and {@code @Analyzer} annotations placed at field level.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class Search5MetadataCreator implements AnnotationMetadataCreator<IndexingMetadata, Descriptor> {

   private static final Log log = Log.getLog(Search5MetadataCreator.class);

   @Override
   public IndexingMetadata create(Descriptor descriptor, AnnotationElement.Annotation annotation) {
      Boolean enabled = (Boolean) annotation.getAttributeValue(InfinispanAnnotations.ENABLED_ATTRIBUTE).getValue();

      String indexName = (String) annotation.getAttributeValue(IndexingMetadata.INDEXED_INDEX_ATTRIBUTE).getValue();
      if (indexName.isEmpty()) {
         indexName = null;
      }

      String entityAnalyzer = null;
      AnnotationElement.Annotation entityAnalyzerAnnotation = findAnnotation(descriptor.getAnnotations(), Search5Annotations.ANALYZER_ANNOTATION);
      if (entityAnalyzerAnnotation != null) {
         String v = (String) entityAnalyzerAnnotation.getAttributeValue(Search5Annotations.ANALYZER_DEFINITION_ATTRIBUTE).getValue();
         if (!v.isEmpty()) {
            entityAnalyzer = v;
         }
      }

      Map<String, SpatialFieldMapping> spatialFields = spatialFields(descriptor);
      Map<String, FieldMapping> fields = fieldsMapping(descriptor, spatialFields);
      String keyEntity = (String) annotation.getAttributeValue(InfinispanAnnotations.KEY_ENTITY_ATTRIBUTE).getValue();
      IndexingKeyMetadata indexingKeyMetadata = null;
      if (!keyEntity.isEmpty()) {
         String keyPropertyName = (String) annotation.getAttributeValue(InfinispanAnnotations.KEY_PROPERTY_NAME_ATTRIBUTE).getValue();
         if (fields.containsKey(keyPropertyName)) {
            throw log.keyPropertyNameAlreadyInUse(keyPropertyName);
         }

         Integer includeDepth = (Integer) annotation.getAttributeValue(InfinispanAnnotations.KEY_INCLUDE_DEPTH_ATTRIBUTE).getValue();
         indexingKeyMetadata = new IndexingKeyMetadata(keyPropertyName, keyEntity, includeDepth);
      }

      IndexingMetadata indexingMetadata = new IndexingMetadata(enabled, indexName, entityAnalyzer, fields, spatialFields, indexingKeyMetadata);
      if (log.isDebugEnabled()) {
         log.debugf("Descriptor name=%s indexingMetadata=%s", descriptor.getFullName(), indexingMetadata);
      }
      return indexingMetadata;
   }

   public static IndexingMetadata createForEmbeddedType(Descriptor descriptor) {
      Map<String, SpatialFieldMapping> spatialFields = spatialFields(descriptor);
      Map<String, FieldMapping> fields = fieldsMapping(descriptor, spatialFields);
      IndexingMetadata indexingMetadata = new IndexingMetadata(false, null, null, fields, spatialFields, null);
      if (log.isDebugEnabled()) {
         log.debugf("Descriptor name=%s indexingMetadata=%s", descriptor.getFullName(), indexingMetadata);
      }
      return indexingMetadata;
   }

   private static Map<String, FieldMapping> fieldsMapping(Descriptor descriptor, Map<String, SpatialFieldMapping> spatialFields) {
      Map<String, FieldMapping> fields = new HashMap<>(descriptor.getFields().size());
      for (FieldDescriptor fd : descriptor.getFields()) {
         Map<String, AnnotationElement.Annotation> annotations = fd.getAnnotations();

         AnnotationElement.Annotation longitudeAnnotation = fd.getAnnotations().get(InfinispanAnnotations.LONGITUDE_ANNOTATION);
         if (longitudeAnnotation != null) {
            processLongitude(fd, longitudeAnnotation, spatialFields);
            continue;
         }

         AnnotationElement.Annotation latitudeAnnotation = fd.getAnnotations().get(InfinispanAnnotations.LATITUDE_ANNOTATION);
         if (latitudeAnnotation != null) {
            processLatitude(fd, latitudeAnnotation, spatialFields);
            continue;
         }

         String fieldName = fd.getName();
         if (spatialFields.containsKey(fieldName)) {
            throw new AnnotationParserException("The spatial index field '" + fieldName + "' clashes with the field declared by " + fd.getFullName());
         }

         FieldMapping fieldMapping = InfinispanMetadataCreator.fieldMapping(fd, annotations);
         if (fieldMapping != null) {
            FieldMapping existingFieldMapping = fields.put(fieldName, fieldMapping);
            if (existingFieldMapping != null) {
               throw new AnnotationParserException("The index field " + fieldName + " was already declared by field " + existingFieldMapping.getFieldDescriptor().getFullName());
            }
            continue;
         }

         SpatialFieldMapping spatialFieldMapping = InfinispanMetadataCreator.geoField(fd, annotations);
         if (spatialFieldMapping != null) {
            spatialFields.put(spatialFieldMapping.fieldName(), spatialFieldMapping);
            continue;
         }

         fieldMapping = search5FieldMapping(fd, annotations);
         if (fieldMapping != null) {
            FieldMapping existingFieldMapping = fields.put(fieldName, fieldMapping);
            if (existingFieldMapping != null) {
               throw new AnnotationParserException("The index field " + fieldName + " was already declared by field " + existingFieldMapping.getFieldDescriptor().getFullName());
            }
         }
      }
      return fields;
   }

   private static Map<String, SpatialFieldMapping> spatialFields(Descriptor descriptor) {
      Map<String, SpatialFieldMapping> spatialFields = new HashMap<>();
      AnnotationElement.Annotation spatialAnnotation = descriptor.getAnnotations().get(InfinispanAnnotations.GEO_POINT_ANNOTATION);
      if (spatialAnnotation != null) {
         SpatialFieldMapping spatial = InfinispanMetadataCreator.geoPoint(spatialAnnotation);
         spatialFields.put(spatial.fieldName(), spatial);
      }
      AnnotationElement.Annotation spatialsAnnotation = descriptor.getAnnotations().get(InfinispanAnnotations.GEO_POINTS_ANNOTATION);
      if (spatialsAnnotation != null) {
         for (AnnotationElement.Value v : ((AnnotationElement.Array) spatialsAnnotation.getDefaultAttributeValue()).getValues()) {
            SpatialFieldMapping spatial = InfinispanMetadataCreator.geoPoint((AnnotationElement.Annotation) v);
            spatialFields.put(spatial.fieldName(), spatial);
         }
      }
      return spatialFields;
   }

   private static FieldMapping search5FieldMapping(FieldDescriptor fd, Map<String, AnnotationElement.Annotation> annotations) {
      String fieldLevelAnalyzer = null;
      AnnotationElement.Annotation fieldAnalyzerAnnotation = findAnnotation(fd.getAnnotations(), Search5Annotations.ANALYZER_ANNOTATION);
      if (fieldAnalyzerAnnotation != null) {
         String v = (String) fieldAnalyzerAnnotation.getAttributeValue(Search5Annotations.ANALYZER_DEFINITION_ATTRIBUTE).getValue();
         if (!v.isEmpty()) {
            fieldLevelAnalyzer = v;
         }
      }

      boolean isSortable = findAnnotation(fd.getAnnotations(), Search5Annotations.SORTABLE_FIELD_ANNOTATION) != null;

      AnnotationElement.Annotation fieldAnnotation = findAnnotation(fd.getAnnotations(), Search5Annotations.FIELD_ANNOTATION);
      if (fieldAnnotation == null) {
         return null;
      }

      String fieldName = fd.getName();
      String v = (String) fieldAnnotation.getAttributeValue(Search5Annotations.FIELD_NAME_ATTRIBUTE).getValue();
      if (!v.isEmpty()) {
         fieldName = v;
      }

      AnnotationElement.Value indexAttribute = fieldAnnotation.getAttributeValue(Search5Annotations.FIELD_INDEX_ATTRIBUTE);
      boolean isIndexed = IndexingMetadata.attributeMatches(indexAttribute, Search5Annotations.LEGACY_ANNOTATION_PACKAGE, Search5Annotations.INDEX_YES, IndexingMetadata.YES);

      AnnotationElement.Value analyzeAttribute = fieldAnnotation.getAttributeValue(Search5Annotations.FIELD_ANALYZE_ATTRIBUTE);
      boolean isAnalyzed = IndexingMetadata.attributeMatches(analyzeAttribute, Search5Annotations.LEGACY_ANNOTATION_PACKAGE, Search5Annotations.ANALYZE_YES, IndexingMetadata.YES);

      AnnotationElement.Value storeAttribute = fieldAnnotation.getAttributeValue(Search5Annotations.FIELD_STORE_ATTRIBUTE);
      boolean isStored = IndexingMetadata.attributeMatches(storeAttribute, Search5Annotations.LEGACY_ANNOTATION_PACKAGE, Search5Annotations.STORE_YES, IndexingMetadata.YES);

      AnnotationElement.Value indexNullAsAttribute = fieldAnnotation.getAttributeValue(Search5Annotations.FIELD_INDEX_NULL_AS_ATTRIBUTE);
      String indexNullAs = (String) indexNullAsAttribute.getValue();
      if (Search5Annotations.DO_NOT_INDEX_NULL.equals(indexNullAs)) {
         indexNullAs = null;
      }

      AnnotationElement.Annotation fieldLevelAnalyzerAnnotationAttribute = (AnnotationElement.Annotation) fieldAnnotation.getAttributeValue(Search5Annotations.FIELD_ANALYZER_ATTRIBUTE).getValue();
      String fieldLevelAnalyzerAttribute = (String) fieldLevelAnalyzerAnnotationAttribute.getAttributeValue(Search5Annotations.ANALYZER_DEFINITION_ATTRIBUTE).getValue();
      if (!fieldLevelAnalyzerAttribute.isEmpty()) {
         // TODO [anistor] field level analyzer attribute overrides the one specified by an eventual field level Analyzer annotation. Need to check if this is consistent with hibernate search
         fieldLevelAnalyzer = fieldLevelAnalyzerAttribute;
      }

      // field level analyzer should not be specified unless the field is analyzed
      if (!isAnalyzed && fieldLevelAnalyzer != null) {
         throw new AnnotationParserException("Cannot specify an analyzer for field " + fd.getFullName() + " unless the field is analyzed.");
      }

      String analyzer = analyzer(fd.getType(), isAnalyzed, fieldLevelAnalyzer);
      boolean sortable = sortable(analyzer, isStored, isSortable);
      FieldMapping fieldMapping = FieldMapping.make(fd, fieldName, isIndexed, isStored, false, sortable)
            .indexNullAs(indexNullAs)
            .analyzer(analyzer)
            .build();

      if (log.isDebugEnabled()) {
         log.debugf("fieldName=%s fieldMapping=%s", fieldName, fieldMapping);
      }

      return fieldMapping;
   }

   private static boolean sortable(String fieldLevelAnalyzer, boolean isStored, boolean isSortable) {
      if (fieldLevelAnalyzer != null) {
         return false;
      }

      return (isSortable || isStored);
   }

   private static String analyzer(Type type, boolean analyze, String fieldLevelAnalyzer) {
      if (!Type.STRING.equals(type) || !analyze) {
         return null;
      }

      return (fieldLevelAnalyzer != null) ? fieldLevelAnalyzer :
            DefaultAnalysisConfigurer.STANDARD_ANALYZER_NAME;
   }

   private static void processLatitude(FieldDescriptor fd, AnnotationElement.Annotation latitudeAnnotation, Map<String, SpatialFieldMapping> spatialFields) {
      String marker = (String) latitudeAnnotation.getAttributeValue(InfinispanAnnotations.GEO_FIELD_NAME_ATTRIBUTE).getValue();
      if (marker.isEmpty()) {
         marker = null;
      }
      SpatialFieldMapping found = null;
      for (SpatialFieldMapping sf : spatialFields.values()) {
         if (Objects.equals(sf.marker(), marker)) {
            if (found != null) {
               throw new AnnotationParserException("Found multiple latitude fields with the same marketSet value " + marker + " for field " + fd.getFullName());
            }
            found = sf;
         }
      }
      if (found == null) {
         throw new AnnotationParserException("No latitude field found with the marketSet value " + marker + " for field " + fd.getFullName());
      }
      found.setLatitude(fd);
   }

   private static void processLongitude(FieldDescriptor fd, AnnotationElement.Annotation longitudeAnnotation, Map<String, SpatialFieldMapping> spatialFields) {
      String marker = (String) longitudeAnnotation.getAttributeValue(InfinispanAnnotations.GEO_FIELD_NAME_ATTRIBUTE).getValue();
      if (marker.isEmpty()) {
         marker = null;
      }
      SpatialFieldMapping found = null;
      for (SpatialFieldMapping sf : spatialFields.values()) {
         if (Objects.equals(sf.marker(), marker)) {
            if (found != null) {
               throw new AnnotationParserException("Found multiple longitude fields with the same marketSet value " + marker + " for field " + fd.getFullName());
            }
            found = sf;
         }
      }
      if (found == null) {
         throw new AnnotationParserException("No longitude field found with the marketSet value " + marker + " for field " + fd.getFullName());
      }
      found.setLongitude(fd);
   }
}
