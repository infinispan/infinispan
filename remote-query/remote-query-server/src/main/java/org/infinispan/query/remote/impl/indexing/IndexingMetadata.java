package org.infinispan.query.remote.impl.indexing;

import java.util.Map;

import org.infinispan.protostream.descriptors.AnnotationElement;
import org.infinispan.protostream.descriptors.Descriptor;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class IndexingMetadata {

   public static final String INDEXED_ANNOTATION = "Indexed";
   public static final String INDEXED_INDEX_ATTRIBUTE = "index";

   public static final String YES = "YES";
   public static final String NO = "NO";

   /**
    * Indicates if the type is indexed.
    */
   private final boolean isIndexed;

   /**
    * The name of the index. Can be {@code null}.
    */
   private final String indexName;

   /**
    * The name of the analyzer. Can be {@code null}.
    */
   private final String analyzer;

   /**
    * Field mappings. This is null if indexing is disabled.
    */
   private final Map<String, FieldMapping> fields;

   public IndexingMetadata(boolean isIndexed, String indexName, String analyzer, Map<String, FieldMapping> fields) {
      this.isIndexed = isIndexed;
      this.indexName = indexName;
      this.analyzer = analyzer;
      this.fields = fields;
   }

   public boolean isIndexed() {
      return isIndexed;
   }

   // TODO [anistor] The index name is ignored for now because all types get indexed in the same index of ProtobufValueWrapper
   public String indexName() {
      return indexName;
   }

   public String analyzer() {
      return analyzer;
   }

   public boolean isFieldSearchable(String fieldName) {
      if (fields == null) {
         return isIndexed;
      }
      FieldMapping fieldMapping = fields.get(fieldName);
      return fieldMapping != null && fieldMapping.searchable();
   }

   public boolean isFieldAnalyzed(String fieldName) {
      if (fields == null) {
         return false;
      }
      FieldMapping fieldMapping = fields.get(fieldName);
      return fieldMapping != null && fieldMapping.analyzed();
   }

   public boolean isFieldProjectable(String fieldName) {
      if (fields == null) {
         return isIndexed;
      }
      FieldMapping fieldMapping = fields.get(fieldName);
      return fieldMapping != null && fieldMapping.projectable();
   }

   public boolean isFieldSortable(String fieldName) {
      if (fields == null) {
         return isIndexed;
      }
      FieldMapping fieldMapping = fields.get(fieldName);
      return fieldMapping != null && fieldMapping.sortable();
   }

   public Object getNullMarker(String fieldName) {
      if (fields == null) {
         return null;
      }
      FieldMapping fieldMapping = fields.get(fieldName);
      return fieldMapping != null ? fieldMapping.indexNullAs() : null;
   }

   public FieldMapping getFieldMapping(String name) {
      if (fields == null) {
         return null;
      }
      return fields.get(name);
   }

   @Override
   public String toString() {
      return "IndexingMetadata{" +
            "isIndexed=" + isIndexed +
            ", indexName='" + indexName + '\'' +
            ", analyzer='" + analyzer + '\'' +
            ", fields=" + fields +
            '}';
   }

   public static AnnotationElement.Annotation findAnnotation(Map<String, AnnotationElement.Annotation> annotations, String name) {
      return annotations.get(name);
   }

   public static <T> T findProcessedAnnotation(Descriptor descriptor, String name) {
      return descriptor.getProcessedAnnotation(name);
   }

   public static boolean attributeMatches(AnnotationElement.Value attr, String packageName, String... validValues) {
      String v = String.valueOf(attr.getValue());
      for(String valid : validValues) {
         if (valid.equals(v) || (packageName+'.'+valid).equals(v)) {
            return true;
         }
      }
      return false;
   }
}
