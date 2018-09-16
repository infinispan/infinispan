package org.infinispan.query.remote.impl.indexing;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.hibernate.search.annotations.Field;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.descriptors.AnnotationElement;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.Option;

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
 * <p>
 * <b>Example:</b>
 * <p>
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
 * The '{@literal @}Indexed' annotation applies to message types only, has a boolean value that defaults to 'true', so
 * '{@literal @}Indexed' is equivalent to '{@literal @}Indexed(true)'. The presence of this annotation indicates the
 * type is to be indexed and we intend to selectively specify which of the fields of this message type are to be indexed.
 * '@Indexed(false)' turns off indexing for this type so the eventual '@Field' annotations present at field level
 * will be ignored. The usage of '@Indexed(false)' is temporarily allowed, it is currently deprecated, and will no
 * longer be supported in Infinispan 10.0 in which the only official way to turn off indexing for a type will be to not
 * annotate it at all. The {@literal @}Indexed annotation also has an optional 'index' attribute which allow you to
 * specify the name of the index for this message type. If left unspecified it defaults to the fully qualified type name.
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
    * A protobuf boolean option that controls 'indexing by default' for message types in current schema file that do not
    * have indexing annotations. This behaviour is active by default and exists only for compatibility with the first
    * release of remote query. It is deprecated and should not be relied upon; a warning message will be logged on every
    * indexing operation that relies on 'indexing by default' behaviour. You are encouraged to turn this behaviour
    * off completely by specifying {@code option indexed_by_default = false;} at the beginning of your schema file.
    * and to annotate your message types in order to properly control indexing.
    * <p>
    * This 'indexing by default' behaviour is transient; it will be removed in a future version and the option that
    * controls it will become deprecated too and will be ignored (and will trigger a deprecation warning message if
    * encountered).
    */
   //TODO [anistor] to be removed in Infinispan 10
   private static final String INDEXED_BY_DEFAULT_OPTION = "indexed_by_default";

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

   //TODO [anistor] remove in Infinispan 10.0
   /**
    * Deprecated since 9.0. Replaced by @Field.
    * This annotation does not have a plural.
    * @deprecated
    */
   @Deprecated
   public static final String INDEXED_FIELD_ANNOTATION = "IndexedField";

   /**
    * @deprecated
    */
   @Deprecated
   public static final String INDEXED_FIELD_INDEX_ATTRIBUTE = "index";

   /**
    * @deprecated
    */
   @Deprecated
   public static final String INDEXED_FIELD_STORE_ATTRIBUTE = "store";

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
    * A special placeholder value that is indexed if the actual field value is {@code null} and no explicit indexing
    * options were defined via protobuf annotations. This placeholder is needed because Lucene does not index actual null
    * values.
    */
   public static final String DEFAULT_NULL_TOKEN = "_null_";

   /**
    * A marker value that indicates nulls should not be indexed. Same string as in Hibernate Search.
    */
   public static final String DO_NOT_INDEX_NULL = Field.DO_NOT_INDEX_NULL;

   /**
    * Similar to org.hibernate.search.annotations.Analyzer. Can be placed at both message and field level.
    */
   public static final String ANALYZER_ANNOTATION = "Analyzer";
   public static final String ANALYZER_DEFINITION_ATTRIBUTE = "definition";

   public static final String SORTABLE_FIELD_ANNOTATION = "SortableField";
   public static final String SORTABLE_FIELDS_ANNOTATION = "SortableFields";

   /**
    * A metadata instance to be used if indexing is disabled for a type.
    */
   public static final IndexingMetadata NO_INDEXING = new IndexingMetadata(false, null, null, null);

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
               // TODO [anistor] the 'value' attribute is deprecated and should be removed in next major version (10.0)
               .attribute(AnnotationElement.Annotation.VALUE_DEFAULT_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(true)
               .attribute(INDEXED_INDEX_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .metadataCreator(new IndexingMetadataCreator())
               .parentBuilder()
            .annotation(ANALYZER_ANNOTATION, AnnotationElement.AnnotationTarget.MESSAGE, AnnotationElement.AnnotationTarget.FIELD)
               .attribute(ANALYZER_DEFINITION_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .parentBuilder()
            .annotation(INDEXED_FIELD_ANNOTATION, AnnotationElement.AnnotationTarget.FIELD)
               .attribute(INDEXED_FIELD_INDEX_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(true)
               .attribute(INDEXED_FIELD_STORE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(true)
               .parentBuilder()
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
                  .defaultValue(ANALYZE_NO)  //todo [anistor] this differs from Hibernate Search default which is Analyze.YES !
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
               .parentBuilder()
            .annotation(SORTABLE_FIELD_ANNOTATION, AnnotationElement.AnnotationTarget.FIELD)
               .repeatable(SORTABLE_FIELDS_ANNOTATION);
   }

   //TODO [anistor] to be removed in Infinispan 10
   /**
    * Retrieves the value of the 'indexed_by_default' protobuf option from the schema file defining the given
    * message descriptor.
    */
   public static boolean isLegacyIndexingEnabled(Descriptor messageDescriptor) {
      boolean isLegacyIndexingEnabled = true;
      for (Option o : messageDescriptor.getFileDescriptor().getOptions()) {
         if (o.getName().equals(INDEXED_BY_DEFAULT_OPTION)) {
            isLegacyIndexingEnabled = Boolean.valueOf((String) o.getValue());
            break;
         }
      }
      return isLegacyIndexingEnabled;
   }
}
