package org.infinispan.query.remote.impl.indexing;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.descriptors.AnnotationElement;

/**
 * All fields of Protobuf types are indexed and stored by default if no indexing annotations are present. This behaviour
 * exists only for compatibility with first release of remote query; it is deprecated and will be removed in Infinispan
 * 10.0 (the lack of annotations on your message/field definition will imply no indexing support in this future release,
 * but you will still be able to perform unindexed query). Indexing all fields is sometimes acceptable but it can become
 * a performance problem if there are many or very large fields. To avoid such problems Infinispan allows and encourages
 * you to specify which fields to index and store by means of two annotations ({@literal @}Indexed and
 * {@literal @}Field) that behave very similarly to the identically named Hibernate Search annotations and which can be
 * directly added to your Protobuf schema files in the documentation comments of your message type definitions as
 * demonstrated in the example below:
 * <p/>
 * <b>Example:</b>
 * <p/>
 * <pre>
 * /**
 *  * This message type is indexed, but not all of its fields are.
 *  *{@literal @}Indexed
 *  *{@literal /}
 * message Note {
 *
 *    /**
 *     * This field is indexed and analyzed but not stored. It can be full-text queried but cannot be used for projections.
 *     *{@literal @}Field(index=Index.YES, store=Store.NO, analyze=Analyze.YES)
 *     *{@literal /}
 *     optional string text = 1;
 *
 *    /**
 *     * A field that is both indexed and stored but not analyzed (the defaults - if no attributes are specified). It can be
 *     * queried with relational operators but not with full-text operators (since it is not analyzed).
 *     *{@literal @}Field
 *     *{@literal /}
 *     optional string author = 2;
 *
 *     /** @Field(index=Index.NO, store=Store.YES) *{@literal /}
 *     optional bool isRead = 3;
 *
 *     /** This field is not annotated, so it is neither indexed nor stored. *{@literal /}
 *     optional int32 priority = 4;
 * }
 * </pre>
 * <p>
 * Documentation annotations can be added after the human-readable text on the last lines of the documentation comment
 * that precedes the element to be annotated (a message type definition or a field definition).
 * The syntax for defining these pseudo-annotations is identical to the one use by the Java language.
 * <p>
 * The '{@literal @}Indexed' annotation applies to message types only. The presence of this annotation indicates the
 * type is to be indexed and we intend to selectively specify which of the fields of this message type are to be indexed.
 * To turn off indexing for a type just do not add '{@literal @}Indexed to its definition.
 * The {@literal @}Indexed annotation has an optional 'index' attribute which allow you to specify the name of the index
 * for this message type. If left unspecified it defaults to the fully qualified type name.
 * <p>
 * The '{@literal @}Field' annotation applies to fields only and has three attributes, 'index', 'store' and 'analyze',
 * which default to {@literal @}Field(index=Index.YES, store=Store.NO, analyze=Analyze.NO). The 'index' attribute
 * indicates whether the field will be indexed, so it can be used for indexed queries, while the 'store' attribute
 * indicates whether the field value is to be stored in the index too, so it becomes useable for projections. The
 * analyze attribute control analysis. Analyzing must be turned on in order to use the field in full-text searches.
 * <p>
 * The '{@literal @}Analyzer' annotation applies to messages and fields and allows you to specify which analyzer to use
 * if analysis was enabled. If has a single attribute name 'definition' which must contain a valid analyzer definition
 * name specified as a String.
 * <p>
 * <b>NOTE:</b>
 * <ul>
 * <li>1. The {@literal @}Field and {@literal @}Analyzer annotations have effect only if the containing message
 * type was annotated as '{@literal @}Indexed' or '{@literal @}Indexed(true)', otherwise they are ignored.
 * </li>
 * <li>2. Unindexed fields can still be queried in non-indexed mode or with hybrid queries.</li>
 * <ul/>
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class IndexingMetadata {

   /**
    * Similar to org.hibernate.search.annotations.Indexed. Indicates if a type will be indexed or not.
    * Has just two attributes:
    * <ul>
    * <li>'index' - the name of the index; if unspecified it defaults to the fully qualified type name</li>
    * <li>'value' - a boolean that indicates if this type is indexed; defaults to {@code true}; this attribute is
    * <b>deprecated</b> and will be removed in Infinispan 10.0. It can be used to turn off indexing but the preferred
    * way to turn off indexing after Infinispan 10.0 will be to just remove the {@literal @}Indexed annotation
    * completely.</li>
    * </ul>
    */
   public static final String INDEXED_ANNOTATION = "Indexed";
   public static final String INDEXED_INDEX_ATTRIBUTE = "index";

   /**
    * Similar to org.hibernate.search.annotations.Fields/Field.
    */
   public static final String FIELDS_ANNOTATION = "Fields";
   public static final String FIELD_ANNOTATION = "Field";
   public static final String FIELD_NAME_ATTRIBUTE = "name";
   public static final String FIELD_INDEX_ATTRIBUTE = "index";
   public static final String FIELD_BOOST_ATTRIBUTE = "boost";
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

   /**
    * The collection of sortable field names. Cab be empty but never {@code null}.
    */
   private final Set<String> sortableFields;

   IndexingMetadata(boolean isIndexed, String indexName, String analyzer, Map<String, FieldMapping> fields) {
      this.isIndexed = isIndexed;
      this.indexName = indexName;
      this.analyzer = analyzer;
      this.fields = fields;
      this.sortableFields = fields == null ? Collections.emptySet() : fields.values().stream()
            .filter(FieldMapping::sortable)
            .map(FieldMapping::name)
            .collect(Collectors.toSet());
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

   public boolean isFieldIndexed(String fieldName) {
      if (fields == null) {
         return isIndexed;
      }
      FieldMapping fieldMapping = fields.get(fieldName);
      return fieldMapping != null && fieldMapping.index();
   }

   public boolean isFieldAnalyzed(String fieldName) {
      if (fields == null) {
         return false;
      }
      FieldMapping fieldMapping = fields.get(fieldName);
      return fieldMapping != null && fieldMapping.analyze();
   }

   public boolean isFieldStored(String fieldName) {
      if (fields == null) {
         return isIndexed;
      }
      FieldMapping fieldMapping = fields.get(fieldName);
      return fieldMapping != null && fieldMapping.store();
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

   public Set<String> getSortableFields() {
      return sortableFields;
   }

   @Override
   public String toString() {
      return "IndexingMetadata{" +
            "isIndexed=" + isIndexed +
            ", indexName='" + indexName + '\'' +
            ", analyzer='" + analyzer + '\'' +
            ", fields=" + fields +
            ", sortableFields=" + sortableFields +
            '}';
   }

   public static void configure(Configuration.Builder builder) {
      builder.annotationsConfig()
            .annotation(INDEXED_ANNOTATION, AnnotationElement.AnnotationTarget.MESSAGE)
               .attribute(INDEXED_INDEX_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .metadataCreator(new IndexingMetadataCreator())
            .annotation(ANALYZER_ANNOTATION, AnnotationElement.AnnotationTarget.MESSAGE, AnnotationElement.AnnotationTarget.FIELD)
               .attribute(ANALYZER_DEFINITION_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
            .annotation(FIELD_ANNOTATION, AnnotationElement.AnnotationTarget.FIELD)
               .repeatable(FIELDS_ANNOTATION)
               .attribute(FIELD_NAME_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .attribute(FIELD_INDEX_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.IDENTIFIER)
                  .allowedValues(INDEX_YES, INDEX_NO)
                  .defaultValue(INDEX_YES)
               .attribute(FIELD_BOOST_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.FLOAT)
                  .defaultValue(1.0f)
               .attribute(FIELD_ANALYZE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.IDENTIFIER)
                  .allowedValues(ANALYZE_YES, ANALYZE_NO)
                  .defaultValue(ANALYZE_NO)  //NOTE: this differs from Hibernate Search's default which is Analyze.YES !
               .attribute(FIELD_STORE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.IDENTIFIER)
                  .allowedValues(STORE_YES, STORE_NO)
                  .defaultValue(STORE_NO)
               .attribute(FIELD_ANALYZER_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.ANNOTATION)
                  .allowedValues(ANALYZER_ANNOTATION)
                  .defaultValue("@Analyzer(definition=\"\")")
               .attribute(FIELD_INDEX_NULL_AS_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue(DO_NOT_INDEX_NULL)
            .annotation(SORTABLE_FIELD_ANNOTATION, AnnotationElement.AnnotationTarget.FIELD)
               .repeatable(SORTABLE_FIELDS_ANNOTATION);
   }
}
