package org.infinispan.query.remote.impl.indexing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.AnnotationMetadataCreator;
import org.infinispan.protostream.AnnotationParserException;
import org.infinispan.protostream.descriptors.AnnotationElement;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.remote.impl.logging.Log;

//todo [anistor] Should be able to have multiple mappings per field like in Hibernate Search, ie. have a @Fields plural annotation

/**
 * {@link AnnotationMetadataCreator} for {@code @Indexed} ProtoStream annotation placed at message type level. Also
 * handles {@code @Field} and {@code @SortableField} and {@code @Analyzer} annotations placed at field level.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
final class IndexingMetadataCreator implements AnnotationMetadataCreator<IndexingMetadata, Descriptor> {

   private static final Log log = LogFactory.getLog(IndexingMetadataCreator.class, Log.class);

   // Recognized annotations:
   // @Indexed ('index' optional string attribute; defaults to empty string)
   // @Analyzer (definition = "<definition name>")
   // @Field (name optional string, index = true/false, defaults to true, analyze = true/false, defaults to true, store = true/false, defaults to false, analyzer = @Analyzer(definition = "<definition name>"))
   // @SortableField
   // @GeoPointBinding
   // @Latitude
   // @Longitude
   @Override
   public IndexingMetadata create(Descriptor descriptor, AnnotationElement.Annotation annotation) {
      String indexName = (String) annotation.getAttributeValue(IndexingMetadata.INDEXED_INDEX_ATTRIBUTE).getValue();
      if (indexName.isEmpty()) {
         indexName = null;
      }

      List<SpatialFieldMapping> spatialFields = new ArrayList<>();
      AnnotationElement.Annotation spatialAnnotation = descriptor.getAnnotations().get(IndexingMetadata.SPATIAL_ANNOTATION);
      if (spatialAnnotation != null) {
         processSpatial(spatialAnnotation, spatialFields);
      }
      AnnotationElement.Annotation spatialsAnnotation = descriptor.getAnnotations().get(IndexingMetadata.SPATIALS_ANNOTATION);
      if (spatialsAnnotation != null) {
         for (AnnotationElement.Value v : ((AnnotationElement.Array) spatialsAnnotation.getDefaultAttributeValue()).getValues()) {
            processSpatial((AnnotationElement.Annotation) v, spatialFields);
         }
      }
      //todo [anistor] more validation, if indexed==false do we accept Spatial annotation?
      //todo [anistor] more validation, if indexed==false do we accept Analyzer annotation?

      String entityAnalyzer = null;
      AnnotationElement.Annotation entityAnalyzerAnnotation = descriptor.getAnnotations().get(IndexingMetadata.ANALYZER_ANNOTATION);
      if (entityAnalyzerAnnotation != null) {
         String v = (String) entityAnalyzerAnnotation.getAttributeValue(IndexingMetadata.ANALYZER_DEFINITION_ATTRIBUTE).getValue();
         if (!v.isEmpty()) {
            entityAnalyzer = v;
         }
      }

      Map<String, FieldMapping> fields = new HashMap<>(descriptor.getFields().size());
      for (FieldDescriptor fd : descriptor.getFields()) {
         AnnotationElement.Annotation longitudeAnnotation = fd.getAnnotations().get(IndexingMetadata.LONGITUDE_ANNOTATION);
         if (longitudeAnnotation != null) {
            processLongitude(fd, longitudeAnnotation, spatialFields);
            //todo [anistor] do we accept other annotations?
            continue;
         }

         AnnotationElement.Annotation latitudeAnnotation = fd.getAnnotations().get(IndexingMetadata.LATITUDE_ANNOTATION);
         if (latitudeAnnotation != null) {
            processLatitude(fd, latitudeAnnotation, spatialFields);
            //todo [anistor] do we accept other annotations?
            continue;
         }

         String fieldLevelAnalyzer = null;
         AnnotationElement.Annotation fieldAnalyzerAnnotation = fd.getAnnotations().get(IndexingMetadata.ANALYZER_ANNOTATION);
         if (fieldAnalyzerAnnotation != null) {
            String v = (String) fieldAnalyzerAnnotation.getAttributeValue(IndexingMetadata.ANALYZER_DEFINITION_ATTRIBUTE).getValue();
            if (!v.isEmpty()) {
               fieldLevelAnalyzer = v;
            }
         }

         boolean isSortable = false;
         AnnotationElement.Annotation sortableFieldAnnotation = fd.getAnnotations().get(IndexingMetadata.SORTABLE_FIELD_ANNOTATION);
         if (sortableFieldAnnotation != null) {
            isSortable = true;
         }

         AnnotationElement.Annotation fieldAnnotation = fd.getAnnotations().get(IndexingMetadata.FIELD_ANNOTATION);
         if (fieldAnnotation != null) {
            String fieldName = fd.getName();
            String v = (String) fieldAnnotation.getAttributeValue(IndexingMetadata.FIELD_NAME_ATTRIBUTE).getValue();
            if (!v.isEmpty()) {
               fieldName = v;
            }

            AnnotationElement.Value indexAttribute = fieldAnnotation.getAttributeValue(IndexingMetadata.FIELD_INDEX_ATTRIBUTE);
            boolean isIndexed = IndexingMetadata.INDEX_YES.equals(indexAttribute.getValue());

            AnnotationElement.Value analyzeAttribute = fieldAnnotation.getAttributeValue(IndexingMetadata.FIELD_ANALYZE_ATTRIBUTE);
            boolean isAnalyzed = IndexingMetadata.ANALYZE_YES.equals(analyzeAttribute.getValue());

            AnnotationElement.Value storeAttribute = fieldAnnotation.getAttributeValue(IndexingMetadata.FIELD_STORE_ATTRIBUTE);
            boolean isStored = IndexingMetadata.STORE_YES.equals(storeAttribute.getValue());

            AnnotationElement.Value indexNullAsAttribute = fieldAnnotation.getAttributeValue(IndexingMetadata.FIELD_INDEX_NULL_AS_ATTRIBUTE);
            String indexNullAs = (String) indexNullAsAttribute.getValue();
            if (IndexingMetadata.DO_NOT_INDEX_NULL.equals(indexNullAs)) {
               indexNullAs = null;
            }

            AnnotationElement.Annotation fieldLevelAnalyzerAnnotationAttribute = (AnnotationElement.Annotation) fieldAnnotation.getAttributeValue(IndexingMetadata.FIELD_ANALYZER_ATTRIBUTE).getValue();
            String fieldLevelAnalyzerAttribute = (String) fieldLevelAnalyzerAnnotationAttribute.getAttributeValue(IndexingMetadata.ANALYZER_DEFINITION_ATTRIBUTE).getValue();
            if (fieldLevelAnalyzerAttribute.isEmpty()) {
               fieldLevelAnalyzerAttribute = null;
            } else {
               // TODO [anistor] field level analyzer attribute overrides the one specified by an eventual field level Analyzer annotation. Need to check if this is consistent with hibernate search
               fieldLevelAnalyzer = fieldLevelAnalyzerAttribute;
            }

            // field level analyzer should not be specified unless the field is analyzed
            if (!isAnalyzed && (fieldLevelAnalyzer != null || fieldLevelAnalyzerAttribute != null)) {
               throw new AnnotationParserException("Cannot specify an analyzer for field " + fd.getFullName() + " unless the field is analyzed.");
            }

            FieldMapping fieldMapping = new FieldMapping(fieldName, isIndexed, isAnalyzed, isStored, isSortable, fieldLevelAnalyzer, indexNullAs, fd);
            FieldMapping existingFieldMapping = fields.put(fieldName, fieldMapping);
            if (existingFieldMapping != null) {
               throw new AnnotationParserException("The index field " + fieldName + " was already declared by field " + existingFieldMapping.getFieldDescriptor().getFullName());
            }
            if (log.isDebugEnabled()) {
               log.debugf("fieldName=%s fieldMapping=%s", fieldName, fieldMapping);
            }
         }
      }

      IndexingMetadata indexingMetadata = new IndexingMetadata(true, indexName, entityAnalyzer, fields, spatialFields);
      if (log.isDebugEnabled()) {
         log.debugf("Descriptor name=%s indexingMetadata=%s", descriptor.getFullName(), indexingMetadata);
      }
      return indexingMetadata;
   }

   private void processSpatial(AnnotationElement.Annotation spatialAnnotation, List<SpatialFieldMapping> spatialFields) {
      String fieldName = (String) spatialAnnotation.getAttributeValue(IndexingMetadata.SPATIAL_FIELD_NAME_ATTRIBUTE).getValue();
      if (fieldName.isEmpty()) {
         fieldName = null;
      }
      String markerSet = (String) spatialAnnotation.getAttributeValue(IndexingMetadata.SPATIAL_MARKER_SET_ATTRIBUTE).getValue();
      if (markerSet.isEmpty()) {
         markerSet = null;
      }
      //todo [anistor] consider also the DEFAULT values
      String projectableStr = (String) spatialAnnotation.getAttributeValue(IndexingMetadata.SPATIAL_PROJECTABLE_ATTRIBUTE).getValue();
      boolean projectable = projectableStr.equals(IndexingMetadata.PROJECTABLE_YES);
      String sortableStr = (String) spatialAnnotation.getAttributeValue(IndexingMetadata.SPATIAL_PROJECTABLE_ATTRIBUTE).getValue();
      boolean sortable = sortableStr.equals(IndexingMetadata.SORTABLE_YES);
      spatialFields.add(new SpatialFieldMapping(fieldName, markerSet, projectable, sortable));
   }

   private void processLatitude(FieldDescriptor fd, AnnotationElement.Annotation latitudeAnnotation, List<SpatialFieldMapping> spatialFields) {
      String markerSet = (String) latitudeAnnotation.getAttributeValue(IndexingMetadata.LATITUDE_MARKERSET_ATTRIBUTE).getValue();
      if (markerSet.isEmpty()) {
         markerSet = null;
      }
      SpatialFieldMapping found = null;
      for (SpatialFieldMapping sf : spatialFields) {
         if (Objects.equals(sf.markerSet(), markerSet)) {
            if (found != null) {
               throw new AnnotationParserException("Found multiple latitude fields with the same marketSet value " + markerSet + " for field " + fd.getFullName());
            }
            found = sf;
         }
      }
      if (found == null) {
         throw new AnnotationParserException("No latitude field found with the marketSet value " + markerSet + " for field " + fd.getFullName());
      }
      found.setLatitude(fd.getName());
   }

   private void processLongitude(FieldDescriptor fd, AnnotationElement.Annotation longitudeAnnotation, List<SpatialFieldMapping> spatialFields) {
      String markerSet = (String) longitudeAnnotation.getAttributeValue(IndexingMetadata.LONGITUDE_MARKERSET_ATTRIBUTE).getValue();
      if (markerSet.isEmpty()) {
         markerSet = null;
      }
      SpatialFieldMapping found = null;
      for (SpatialFieldMapping sf : spatialFields) {
         if (Objects.equals(sf.markerSet(), markerSet)) {
            if (found != null) {
               throw new AnnotationParserException("Found multiple longitude fields with the same marketSet value " + markerSet + " for field " + fd.getFullName());
            }
            found = sf;
         }
      }
      if (found == null) {
         throw new AnnotationParserException("No longitude field found with the marketSet value " + markerSet + " for field " + fd.getFullName());
      }
      found.setLongitude(fd.getName());
   }
}
