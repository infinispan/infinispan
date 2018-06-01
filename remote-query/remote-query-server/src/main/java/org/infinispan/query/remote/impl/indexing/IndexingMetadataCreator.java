package org.infinispan.query.remote.impl.indexing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.document.Field;
import org.hibernate.search.annotations.SpatialMode;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.LuceneOptions;
import org.hibernate.search.engine.impl.LuceneOptionsImpl;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.AnnotationMetadataCreator;
import org.infinispan.protostream.descriptors.AnnotationElement;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.remote.impl.logging.Log;
import org.jboss.logging.Logger;

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
   // @Indexed (default 'value' boolean argument; defaults to true; the 'value' argument is deprecated, 'index' optional string attribute; defaults to empty string)
   // @Analyzer (definition = "<definition name>")
   // @IndexedField (index = true/false; defaults to true, store = true/false; defaults to true) - deprecated annotation, prefer @Field instead
   // @Field (name optional string, index = true/false, defaults to true, analyze = true/false, defaults to true, store = true/false, defaults to false, analyzer = @Analyzer(definition = "<definition name>"))
   // @SortableField
   @Override
   public IndexingMetadata create(Descriptor descriptor, AnnotationElement.Annotation annotation) {
      AnnotationElement.Value indexedValue = annotation.getDefaultAttributeValue();
      if (Boolean.TRUE.equals(indexedValue.getValue())) {
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
         //todo [anistor] if indexed==false we do not accept Spatial annotation
         //todo [anistor] if indexed==false do we accept Analyzer ??
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
            if (longitudeAnnotation != null) {
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

               AnnotationElement.Value boostAttribute = fieldAnnotation.getAttributeValue(IndexingMetadata.FIELD_BOOST_ATTRIBUTE);
               float fieldLevelBoost = (Float) boostAttribute.getValue();

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
                  throw new IllegalStateException("Cannot specify an analyzer for field " + fd.getFullName() + " unless the field is analyzed.");
               }

               //TODO [anistor] what is the 'inherited boost'?
               LuceneOptions luceneOptions = new LuceneOptionsImpl(Field.Index.toIndex(isIndexed, isAnalyzed),
                     Field.TermVector.NO,
                     isStored ? Store.YES : Store.NO,
                     indexNullAs,
                     fieldLevelBoost, 1.0f);

               FieldMapping fieldMapping = new FieldMapping(fieldName, isIndexed, fieldLevelBoost, isAnalyzed, isStored, isSortable, fieldLevelAnalyzer, indexNullAs, luceneOptions, fd, false);
               fields.put(fieldName, fieldMapping);
               if (log.isEnabled(Logger.Level.DEBUG)) {
                  log.debugf("fieldName=%s fieldMapping=%s", fieldName, fieldMapping);
               }
            }

            // process the deprecated @IndexedField annotation if present
            AnnotationElement.Annotation indexedFieldAnnotation = fd.getAnnotations().get(IndexingMetadata.INDEXED_FIELD_ANNOTATION);
            if (indexedFieldAnnotation != null) {
               if (log.isEnabled(Logger.Level.WARN)) {
                  log.warnf("Detected usage of deprecated annotation '%s' on field %s. Please consider replacing it with "
                        + IndexingMetadata.FIELD_ANNOTATION + ".", IndexingMetadata.INDEXED_FIELD_ANNOTATION, fd.getFullName());
               }
               if (fieldAnnotation != null) {
                  throw new IllegalStateException("Annotation '" + IndexingMetadata.INDEXED_FIELD_ANNOTATION +
                        "' cannot be used together with '" + IndexingMetadata.FIELD_ANNOTATION + "' on field " + fd.getFullName());
               }
               // The old way of treating the null token as string regardless of the actual field type makes it impossible to work with @SortableField
               if (isSortable) {
                  throw new IllegalStateException("Annotation '" + IndexingMetadata.INDEXED_FIELD_ANNOTATION +
                        "' cannot be used together with '" + IndexingMetadata.SORTABLE_FIELD_ANNOTATION + "' on field " + fd.getFullName());
               }
               if (fieldAnalyzerAnnotation != null) {
                  throw new IllegalStateException("Annotation '" + IndexingMetadata.INDEXED_FIELD_ANNOTATION +
                        "' cannot be used together with '" + IndexingMetadata.ANALYZER_ANNOTATION + "' on field " + fd.getFullName());
               }

               AnnotationElement.Value indexAttribute = indexedFieldAnnotation.getAttributeValue(IndexingMetadata.INDEXED_FIELD_INDEX_ATTRIBUTE);
               boolean isIndexed = Boolean.TRUE.equals(indexAttribute.getValue());
               AnnotationElement.Value storeAttribute = indexedFieldAnnotation.getAttributeValue(IndexingMetadata.INDEXED_FIELD_STORE_ATTRIBUTE);
               boolean isStored = Boolean.TRUE.equals(storeAttribute.getValue());
               String indexNullAs = IndexingMetadata.DEFAULT_NULL_TOKEN;
               LuceneOptions luceneOptions = new LuceneOptionsImpl(isIndexed ? Field.Index.NOT_ANALYZED : Field.Index.NO,
                     Field.TermVector.NO,
                     isStored ? Store.YES : Store.NO,
                     indexNullAs,
                     1.0f, 1.0f);

               FieldMapping fieldMapping = new FieldMapping(fd.getName(), isIndexed, 1.0f, false, isStored, false, null, indexNullAs, luceneOptions, fd, true);
               fields.put(fd.getName(), fieldMapping);
               if (log.isEnabled(Logger.Level.DEBUG)) {
                  log.debugf("fieldName=%s fieldMapping=%s", fd.getName(), fieldMapping);
               }
            }
         }

         IndexingMetadata indexingMetadata = new IndexingMetadata(true, indexName, entityAnalyzer, fields, spatialFields);
         if (log.isEnabled(Logger.Level.DEBUG)) {
            log.debugf("Descriptor name=%s indexingMetadata=%s", descriptor.getFullName(), indexingMetadata);
         }
         return indexingMetadata;
      } else {
         return IndexingMetadata.NO_INDEXING;
      }
   }

   private void processSpatial(AnnotationElement.Annotation spatialAnnotation, List<SpatialFieldMapping> spatialFields) {
      String spatialName = (String) spatialAnnotation.getAttributeValue(IndexingMetadata.SPATIAL_NAME_ATTRIBUTE).getValue();
      if (spatialName.isEmpty()) {
         spatialName = null;
      }
      String spatialModeStr = (String) spatialAnnotation.getAttributeValue(IndexingMetadata.SPATIAL_MODE_ATTRIBUTE).getValue();
      SpatialMode spatialMode = spatialModeStr.equals(IndexingMetadata.SPATIAL_MODE_HASH) ? SpatialMode.HASH : SpatialMode.RANGE;
      String spatialStoreStr = (String) spatialAnnotation.getAttributeValue(IndexingMetadata.SPATIAL_STORE_ATTRIBUTE).getValue();
      boolean spatialStore = spatialStoreStr.equals(IndexingMetadata.STORE_YES);
      spatialFields.add(new SpatialFieldMapping(spatialName, spatialMode, spatialStore));
   }

   private void processLatitude(FieldDescriptor fd, AnnotationElement.Annotation latitudeAnnotation, List<SpatialFieldMapping> spatialFields) {
      String of = (String) latitudeAnnotation.getAttributeValue(IndexingMetadata.LATITUDE_OF_ATTRIBUTE).getValue();
      if (of.isEmpty()) {
         of = null;
      }
      for (SpatialFieldMapping sf : spatialFields) {
         if (Objects.equals(sf.name(), of)) {
            sf.setLatitude(fd.getName());
            break;
         }
      }
   }

   private void processLongitude(FieldDescriptor fd, AnnotationElement.Annotation longitudeAnnotation, List<SpatialFieldMapping> spatialFields) {
      String of = (String) longitudeAnnotation.getAttributeValue(IndexingMetadata.LONGITUDE_OF_ATTRIBUTE).getValue();
      if (of.isEmpty()) {
         of = null;
      }
      for (SpatialFieldMapping sf : spatialFields) {
         if (Objects.equals(sf.name(), of)) {
            sf.setLongitude(fd.getName());
            break;
         }
      }
   }
}
