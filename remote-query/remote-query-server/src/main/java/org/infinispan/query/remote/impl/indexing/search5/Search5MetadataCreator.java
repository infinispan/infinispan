package org.infinispan.query.remote.impl.indexing.search5;

import static org.infinispan.query.remote.impl.indexing.IndexingMetadata.findAnnotation;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.AnnotationMetadataCreator;
import org.infinispan.protostream.descriptors.AnnotationElement;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.Type;
import org.infinispan.query.remote.impl.indexing.FieldMapping;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;
import org.infinispan.query.remote.impl.indexing.infinispan.InfinispanAnnotations;
import org.infinispan.query.remote.impl.indexing.infinispan.InfinispanMetadataCreator;
import org.infinispan.query.remote.impl.logging.Log;
import org.infinispan.search.mapper.mapping.impl.DefaultAnalysisConfigurer;

//todo [anistor] Should be able to have multiple mappings per field like in Hibernate Search, ie. have a @Fields plural annotation

/**
 * {@link AnnotationMetadataCreator} for {@code @Indexed} ProtoStream annotation placed at message type level. Also
 * handles {@code @Field} and {@code @SortableField} and {@code @Analyzer} annotations placed at field level.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
final class Search5MetadataCreator implements AnnotationMetadataCreator<IndexingMetadata, Descriptor> {

   private static final Log log = LogFactory.getLog(Search5MetadataCreator.class, Log.class);

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

      Map<String, FieldMapping> fields = new HashMap<>(descriptor.getFields().size());
      for (FieldDescriptor fd : descriptor.getFields()) {
         Map<String, AnnotationElement.Annotation> annotations = fd.getAnnotations();

         FieldMapping fieldMapping = InfinispanMetadataCreator.fieldMapping(fd, annotations);
         if (fieldMapping != null) {
            fields.put(fd.getName(), fieldMapping);
            continue;
         }

         fieldMapping = search5FieldMapping(fd, annotations);
         if (fieldMapping != null) {
            fields.put(fd.getName(), fieldMapping);
         }
      }

      IndexingMetadata indexingMetadata = new IndexingMetadata(enabled, indexName, entityAnalyzer, fields);
      if (log.isDebugEnabled()) {
         log.debugf("Descriptor name=%s indexingMetadata=%s", descriptor.getFullName(), indexingMetadata);
      }
      return indexingMetadata;
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

      String analyzer = analyzer(fd.getType(), isAnalyzed, fieldLevelAnalyzer);
      FieldMapping fieldMapping = new FieldMapping(
            fieldName,
            isIndexed, isStored, false, sortable(analyzer, isStored, isSortable),
            analyzer, null, indexNullAs, fd);

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
}
