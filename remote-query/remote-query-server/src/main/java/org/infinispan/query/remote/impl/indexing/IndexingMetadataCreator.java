package org.infinispan.query.remote.impl.indexing;

import static org.infinispan.query.remote.impl.indexing.IndexingMetadata.LEGACY_ANNOTATION_PACKAGE;
import static org.infinispan.query.remote.impl.indexing.IndexingMetadata.findAnnotation;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.AnnotationMetadataCreator;
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
   @Override
   public IndexingMetadata create(Descriptor descriptor, AnnotationElement.Annotation annotation) {
      String indexName = (String) annotation.getAttributeValue(IndexingMetadata.INDEXED_INDEX_ATTRIBUTE).getValue();
      if (indexName.isEmpty()) {
         indexName = null;
      }

      String entityAnalyzer = null;
      AnnotationElement.Annotation entityAnalyzerAnnotation = findAnnotation(descriptor.getAnnotations(), IndexingMetadata.ANALYZER_ANNOTATION);
      if (entityAnalyzerAnnotation != null) {
         String v = (String) entityAnalyzerAnnotation.getAttributeValue(IndexingMetadata.ANALYZER_DEFINITION_ATTRIBUTE).getValue();
         if (!v.isEmpty()) {
            entityAnalyzer = v;
         }
      }

      Map<String, FieldMapping> fields = new HashMap<>(descriptor.getFields().size());
      for (FieldDescriptor fd : descriptor.getFields()) {
         String fieldLevelAnalyzer = null;
         AnnotationElement.Annotation fieldAnalyzerAnnotation = findAnnotation(fd.getAnnotations(), IndexingMetadata.ANALYZER_ANNOTATION);
         if (fieldAnalyzerAnnotation != null) {
            String v = (String) fieldAnalyzerAnnotation.getAttributeValue(IndexingMetadata.ANALYZER_DEFINITION_ATTRIBUTE).getValue();
            if (!v.isEmpty()) {
               fieldLevelAnalyzer = v;
            }
         }

         boolean isSortable = findAnnotation(fd.getAnnotations(), IndexingMetadata.SORTABLE_FIELD_ANNOTATION) != null;

         AnnotationElement.Annotation fieldAnnotation = findAnnotation(fd.getAnnotations(), IndexingMetadata.FIELD_ANNOTATION);
         if (fieldAnnotation != null) {
            String fieldName = fd.getName();
            String v = (String) fieldAnnotation.getAttributeValue(IndexingMetadata.FIELD_NAME_ATTRIBUTE).getValue();
            if (!v.isEmpty()) {
               fieldName = v;
            }

            AnnotationElement.Value indexAttribute = fieldAnnotation.getAttributeValue(IndexingMetadata.FIELD_INDEX_ATTRIBUTE);
            boolean isIndexed = attributeMatches(indexAttribute, LEGACY_ANNOTATION_PACKAGE, IndexingMetadata.INDEX_YES, IndexingMetadata.YES);

            AnnotationElement.Value analyzeAttribute = fieldAnnotation.getAttributeValue(IndexingMetadata.FIELD_ANALYZE_ATTRIBUTE);
            boolean isAnalyzed = attributeMatches(analyzeAttribute, LEGACY_ANNOTATION_PACKAGE, IndexingMetadata.ANALYZE_YES, IndexingMetadata.YES);

            AnnotationElement.Value storeAttribute = fieldAnnotation.getAttributeValue(IndexingMetadata.FIELD_STORE_ATTRIBUTE);
            boolean isStored = attributeMatches(storeAttribute, LEGACY_ANNOTATION_PACKAGE, IndexingMetadata.STORE_YES, IndexingMetadata.YES);

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

            FieldMapping fieldMapping = new FieldMapping(fieldName, isIndexed, isAnalyzed, isStored, isSortable, fieldLevelAnalyzer, indexNullAs, fd);
            fields.put(fieldName, fieldMapping);
            if (log.isDebugEnabled()) {
               log.debugf("fieldName=%s fieldMapping=%s", fieldName, fieldMapping);
            }
         }
      }

      IndexingMetadata indexingMetadata = new IndexingMetadata(true, indexName, entityAnalyzer, fields);
      if (log.isDebugEnabled()) {
         log.debugf("Descriptor name=%s indexingMetadata=%s", descriptor.getFullName(), indexingMetadata);
      }
      return indexingMetadata;
   }

   private static boolean attributeMatches(AnnotationElement.Value attr, String packageName, String... validValues) {
      String v = String.valueOf(attr.getValue());
      for(String valid : validValues) {
         if (valid.equals(v) || (packageName+'.'+valid).equals(v)) {
            return true;
         }
      }
      return false;
   }
}
