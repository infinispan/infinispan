package org.infinispan.query.remote.impl.indexing.infinispan;

import java.util.ArrayList;

import org.infinispan.api.annotations.indexing.model.Values;
import org.infinispan.api.annotations.indexing.option.Structure;
import org.infinispan.api.annotations.indexing.option.TermVector;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.descriptors.AnnotationElement;

public class InfinispanAnnotations {

   public static final String ANNOTATIONS_OPTIONS_PACKAGE = "org.infinispan.api.annotations.indexing.option";

   public static final String ENABLED_ATTRIBUTE = "enabled";

   public static final String BASIC_ANNOTATION = "Basic";
   public static final String NAME_ATTRIBUTE = "name";
   public static final String SEARCHABLE_ATTRIBUTE = "searchable";
   public static final String PROJECTABLE_ATTRIBUTE = "projectable";
   public static final String AGGREGABLE_ATTRIBUTE = "aggregable";
   public static final String SORTABLE_ATTRIBUTE = "sortable";
   public static final String INDEX_NULL_AS_ATTRIBUTE = "indexNullAs";

   public static final String KEYWORD_ANNOTATION = "Keyword";
   public static final String TEXT_ANNOTATION = "Text";
   public static final String NORMALIZER_ATTRIBUTE = "normalizer";
   public static final String ANALYZER_ATTRIBUTE = "analyzer";
   public static final String SEARCH_ANALYZER_ATTRIBUTE = "searchAnalyzer";
   public static final String NORMS_ATTRIBUTE = "norms";
   public static final String TERM_VECTOR_ATTRIBUTE = "termVector";

   public static final String DECIMAL_ANNOTATION = "Decimal";
   public static final String DECIMAL_SCALE_ATTRIBUTE = "decimalScale";

   public static final String EMBEDDED_ANNOTATION = "Embedded";
   public static final String INCLUDE_DEPTH_ATTRIBUTE = "includeDepth";
   public static final String STRUCTURE_ATTRIBUTE = "structure";

   public static void configure(Configuration.Builder builder) {
      builder.annotationsConfig()
            .annotation(BASIC_ANNOTATION, AnnotationElement.AnnotationTarget.FIELD)
               .attribute(NAME_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .attribute(SEARCHABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(true)
               .attribute(PROJECTABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(false)
               .attribute(AGGREGABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(false)
               .attribute(SORTABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(false)
               .attribute(INDEX_NULL_AS_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue(Values.DO_NOT_INDEX_NULL)
            .annotation(KEYWORD_ANNOTATION, AnnotationElement.AnnotationTarget.FIELD)
               .attribute(NAME_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .attribute(SEARCHABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(true)
               .attribute(PROJECTABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(false)
               .attribute(AGGREGABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(false)
               .attribute(SORTABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(false)
               .attribute(INDEX_NULL_AS_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue(Values.DO_NOT_INDEX_NULL)
               .attribute(NORMALIZER_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .attribute(NORMS_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(true)
            .annotation(TEXT_ANNOTATION, AnnotationElement.AnnotationTarget.FIELD)
               .attribute(NAME_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .attribute(SEARCHABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(true)
               .attribute(PROJECTABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(false)
               .attribute(ANALYZER_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("standard")
               .attribute(SEARCH_ANALYZER_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .attribute(NORMS_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(true)
               .attribute(TERM_VECTOR_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.IDENTIFIER)
                  .packageName(ANNOTATIONS_OPTIONS_PACKAGE)
                  .allowedValues(termVectorAllowedValues())
                  .defaultValue(TermVector.NO.name())
            .annotation(DECIMAL_ANNOTATION, AnnotationElement.AnnotationTarget.FIELD)
               .attribute(NAME_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .attribute(SEARCHABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(true)
               .attribute(PROJECTABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(false)
               .attribute(AGGREGABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(false)
               .attribute(SORTABLE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.BOOLEAN)
                  .defaultValue(false)
               .attribute(INDEX_NULL_AS_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue(Values.DO_NOT_INDEX_NULL)
               .attribute(DECIMAL_SCALE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.INT)
                  .defaultValue(2)
            .annotation(EMBEDDED_ANNOTATION, AnnotationElement.AnnotationTarget.FIELD)
               .attribute(NAME_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.STRING)
                  .defaultValue("")
               .attribute(INCLUDE_DEPTH_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.INT)
                  .defaultValue(3)
               .attribute(STRUCTURE_ATTRIBUTE)
                  .type(AnnotationElement.AttributeType.IDENTIFIER)
                  .packageName(ANNOTATIONS_OPTIONS_PACKAGE)
                  .allowedValues(structureAllowedValues())
                  .defaultValue(Structure.NESTED.name());
   }

   public static final TermVector termVector(String value) {
      int beginIndex = value.lastIndexOf(".");
      if (beginIndex >= 0) {
         value = value.substring(beginIndex + 1);
      }
      return TermVector.valueOf(value);
   }

   public static final Structure structure(String value) {
      int beginIndex = value.lastIndexOf(".");
      if (beginIndex >= 0) {
         value = value.substring(beginIndex + 1);
      }
      return Structure.valueOf(value);
   }

   private static final String[] termVectorAllowedValues() {
      int capacity = TermVector.values().length * 2;
      ArrayList<String> result = new ArrayList<>(capacity);
      for (TermVector value : TermVector.values()) {
         result.add(value.name());
         result.add("TermVector." + value.name());
      }
      return result.toArray(new String[capacity]);
   }

   private static final String[] structureAllowedValues() {
      int capacity = Structure.values().length * 2;
      ArrayList<String> result = new ArrayList<>(capacity);
      for (Structure value : Structure.values()) {
         result.add(value.name());
         result.add("Structure." + value.name());
      }
      return result.toArray(new String[capacity]);
   }
}
