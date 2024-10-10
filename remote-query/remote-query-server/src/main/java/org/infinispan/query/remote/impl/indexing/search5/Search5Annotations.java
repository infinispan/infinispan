package org.infinispan.query.remote.impl.indexing.search5;

import org.infinispan.api.annotations.indexing.model.Values;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.descriptors.AnnotationElement;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;
import org.infinispan.query.remote.impl.indexing.infinispan.InfinispanAnnotations;

public final class Search5Annotations {

   public static final String LEGACY_ANNOTATION_PACKAGE = "org.hibernate.search.annotations";

   /**
    * Similar to org.hibernate.search.annotations.Fields/Field.
    */
   public static final String FIELDS_ANNOTATION = "Fields"; // the Repeatable container annotation for Field
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

   public static final String PROJECTABLE_DEFAULT = "Projectable.DEFAULT";  // translates to either YES or NO depending on the indexing backend
   public static final String PROJECTABLE_NO = "Projectable.NO";
   public static final String PROJECTABLE_YES = "Projectable.YES";

   public static final String SORTABLE_DEFAULT = "Sortable.DEFAULT";  // translates to either YES or NO depending on the indexing backend
   public static final String SORTABLE_NO = "Sortable.NO";
   public static final String SORTABLE_YES = "Sortable.YES";

   /**
    * Similar to org.hibernate.search.mapper.pojo.bridge.builtin.annotation.GeoPointBinding.
    */
   public static final String SPATIALS_ANNOTATION = "GeoPointBindings"; // the Repeatable for GeoPointBinding
   public static final String SPATIAL_ANNOTATION = "GeoPointBinding";
   public static final String SPATIAL_FIELD_NAME_ATTRIBUTE = "fieldName";
   public static final String SPATIAL_MARKER_SET_ATTRIBUTE = "markerSet";
   public static final String SPATIAL_PROJECTABLE_ATTRIBUTE = "projectable";
   public static final String SPATIAL_SORTABLE_ATTRIBUTE = "sortable";

   /**
    * Similar to org.hibernate.search.mapper.pojo.bridge.builtin.annotation.Latitude.
    */
   public static final String LATITUDE_ANNOTATION = "Latitude";
   public static final String LATITUDE_MARKERSET_ATTRIBUTE = "markerSet";

   /**
    * Similar to org.hibernate.search.mapper.pojo.bridge.builtin.annotation.Longitude.
    */
   public static final String LONGITUDE_ANNOTATION = "Longitude";
   public static final String LONGITUDE_MARKERSET_ATTRIBUTE = "markerSet";

   /**
    * A marker value that indicates nulls should not be indexed.
    */
   public static final String DO_NOT_INDEX_NULL = "__DO_NOT_INDEX_NULL__";

   /**
    * Similar to org.hibernate.search.annotations.Analyzer. Can be placed at both message and field level.
    */
   public static final String ANALYZER_ANNOTATION = "Analyzer";
   public static final String ANALYZER_DEFINITION_ATTRIBUTE = "definition";

   /**
    * Similar to org.hibernate.search.annotations.SortableField/SortableFields.
    */
   public static final String SORTABLE_FIELD_ANNOTATION = "SortableField";
   public static final String SORTABLE_FIELDS_ANNOTATION = "SortableFields"; // the Repeatable container annotation for SortableField

   public static void configure(Configuration.Builder builder) {
      builder.annotationsConfig()
            .annotation(IndexingMetadata.INDEXED_ANNOTATION, AnnotationElement.AnnotationTarget.MESSAGE)
               .packageName(LEGACY_ANNOTATION_PACKAGE)
               .attribute(IndexingMetadata.INDEXED_INDEX_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .attribute(InfinispanAnnotations.ENABLED_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(true)
               .attribute(InfinispanAnnotations.KEY_ENTITY_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .attribute(InfinispanAnnotations.KEY_PROPERTY_NAME_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue(Values.DEFAULT_KEY_PROPERTY_NAME)
               .attribute(InfinispanAnnotations.KEY_INCLUDE_DEPTH_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.INT)
                  .defaultValue(Values.DEFAULT_INCLUDE_DEPTH)
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
               .repeatable(SORTABLE_FIELDS_ANNOTATION)
            .annotation(SPATIAL_ANNOTATION, AnnotationElement.AnnotationTarget.MESSAGE)
               .repeatable(SPATIALS_ANNOTATION)
               .attribute(SPATIAL_FIELD_NAME_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .attribute(SPATIAL_MARKER_SET_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .attribute(SPATIAL_PROJECTABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.IDENTIFIER)
                  .allowedValues(PROJECTABLE_DEFAULT, PROJECTABLE_NO, PROJECTABLE_YES)
                  .defaultValue(PROJECTABLE_DEFAULT)
               .attribute(SPATIAL_SORTABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.IDENTIFIER)
                  .allowedValues(SORTABLE_DEFAULT, SORTABLE_NO, SORTABLE_YES)
                  .defaultValue(SORTABLE_DEFAULT)
                  .annotation(LATITUDE_ANNOTATION, AnnotationElement.AnnotationTarget.FIELD)
               .attribute(LATITUDE_MARKERSET_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
                  .annotation(LONGITUDE_ANNOTATION, AnnotationElement.AnnotationTarget.FIELD)
               .attribute(LONGITUDE_MARKERSET_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("");
   }
}
