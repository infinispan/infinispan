package org.infinispan.query.remote.impl.indexing.search5;

import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.descriptors.AnnotationElement;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;

public final class Search5Annotations {

   public static final String LEGACY_ANNOTATION_PACKAGE = "org.hibernate.search.annotations";

   /**
    * Similar to org.hibernate.search.annotations.Fields/Field.
    */
   public static final String FIELDS_ANNOTATION = "Fields";
   public static final String FIELD_ANNOTATION = "Field";
   public static final String FIELD_NAME_ATTRIBUTE = "name";
   public static final String FIELD_INDEX_ATTRIBUTE = "index";
   public static final String FIELD_ANALYZE_ATTRIBUTE = "analyze";
   public static final String FIELD_STORE_ATTRIBUTE = "store";
   public static final String FIELD_ANALYZER_ATTRIBUTE = "analyzer";
   public static final String FIELD_INDEX_NULL_AS_ATTRIBUTE = "indexNullAs";

   public static final String INDEX_YES = "Index.YES";
   public static final String INDEX_NO = "Index.NO";

   public static final String ANALYZE_YES = "Analyze.YES";
   public static final String ANALYZE_NO = "Analyze.NO";

   public static final String STORE_YES = "Store.YES";
   public static final String STORE_NO = "Store.NO";

   /**
    * A marker value that indicates nulls should not be indexed.
    */
   public static final String DO_NOT_INDEX_NULL = "__DO_NOT_INDEX_NULL__";

   /**
    * Similar to org.hibernate.search.annotations.Analyzer. Can be placed at both message and field level.
    */
   public static final String ANALYZER_ANNOTATION = "Analyzer";
   public static final String ANALYZER_DEFINITION_ATTRIBUTE = "definition";

   public static final String SORTABLE_FIELD_ANNOTATION = "SortableField";
   public static final String SORTABLE_FIELDS_ANNOTATION = "SortableFields";

   public static void configure(Configuration.Builder builder) {
      builder.annotationsConfig()
            .annotation(IndexingMetadata.INDEXED_ANNOTATION, AnnotationElement.AnnotationTarget.MESSAGE)
               .packageName(LEGACY_ANNOTATION_PACKAGE)
               .attribute(IndexingMetadata.INDEXED_INDEX_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .metadataCreator(new Search5MetadataCreator())
            .annotation(ANALYZER_ANNOTATION, AnnotationElement.AnnotationTarget.MESSAGE, AnnotationElement.AnnotationTarget.FIELD)
               .packageName(LEGACY_ANNOTATION_PACKAGE)
                  .attribute(ANALYZER_DEFINITION_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
            .annotation(FIELD_ANNOTATION, AnnotationElement.AnnotationTarget.FIELD)
               .packageName(LEGACY_ANNOTATION_PACKAGE)
               .repeatable(FIELDS_ANNOTATION)
               .attribute(FIELD_NAME_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .attribute(FIELD_INDEX_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.IDENTIFIER)
                  .packageName(LEGACY_ANNOTATION_PACKAGE)
                  .allowedValues(INDEX_YES, INDEX_NO, IndexingMetadata.YES, IndexingMetadata.NO)
                  .defaultValue(INDEX_YES)
               .attribute(FIELD_ANALYZE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.IDENTIFIER)
                  .packageName(LEGACY_ANNOTATION_PACKAGE)
                  .allowedValues(ANALYZE_YES, ANALYZE_NO, IndexingMetadata.YES, IndexingMetadata.NO)
                  .defaultValue(ANALYZE_NO)  //NOTE: this differs from Hibernate Search's default which is Analyze.YES !
               .attribute(FIELD_STORE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.IDENTIFIER)
                  .packageName(LEGACY_ANNOTATION_PACKAGE)
                  .allowedValues(STORE_YES, STORE_NO, IndexingMetadata.YES, IndexingMetadata.NO)
                  .defaultValue(STORE_NO)
               .attribute(FIELD_ANALYZER_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.ANNOTATION)
                  .allowedValues(ANALYZER_ANNOTATION)
                  .defaultValue("@Analyzer(definition=\"\")")
               .attribute(FIELD_INDEX_NULL_AS_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue(DO_NOT_INDEX_NULL)
            .annotation(SORTABLE_FIELD_ANNOTATION, AnnotationElement.AnnotationTarget.FIELD)
               .packageName(LEGACY_ANNOTATION_PACKAGE)
               .repeatable(SORTABLE_FIELDS_ANNOTATION);
   }
}
