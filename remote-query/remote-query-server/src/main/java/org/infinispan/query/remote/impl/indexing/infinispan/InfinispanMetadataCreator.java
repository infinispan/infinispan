package org.infinispan.query.remote.impl.indexing.infinispan;

import java.util.Map;

import org.infinispan.api.annotations.indexing.model.Values;
import org.infinispan.api.annotations.indexing.option.Structure;
import org.infinispan.api.annotations.indexing.option.TermVector;
import org.infinispan.protostream.descriptors.AnnotationElement;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.remote.impl.indexing.FieldMapping;

public final class InfinispanMetadataCreator {

   public static FieldMapping fieldMapping(FieldDescriptor fieldDescriptor, Map<String, AnnotationElement.Annotation> annotations) {
      AnnotationElement.Annotation fieldAnnotation = annotations.get(InfinispanAnnotations.BASIC_ANNOTATION);
      if (fieldAnnotation != null) {
         return basic(fieldDescriptor, fieldAnnotation);
      }

      fieldAnnotation = annotations.get(InfinispanAnnotations.KEYWORD_ANNOTATION);
      if (fieldAnnotation != null) {
         return keyword(fieldDescriptor, fieldAnnotation);
      }

      fieldAnnotation = annotations.get(InfinispanAnnotations.TEXT_ANNOTATION);
      if (fieldAnnotation != null) {
         return text(fieldDescriptor, fieldAnnotation);
      }

      fieldAnnotation = annotations.get(InfinispanAnnotations.DECIMAL_ANNOTATION);
      if (fieldAnnotation != null) {
         return decimal(fieldDescriptor, fieldAnnotation);
      }

      fieldAnnotation = annotations.get(InfinispanAnnotations.EMBEDDED_ANNOTATION);
      if (fieldAnnotation != null) {
         return embedded(fieldDescriptor, fieldAnnotation);
      }

      return null;
   }

   private static FieldMapping basic(FieldDescriptor fieldDescriptor, AnnotationElement.Annotation fieldAnnotation) {
      String name = name(fieldDescriptor, fieldAnnotation);
      String indexNullAs = indexNullAs(fieldAnnotation);

      Boolean searchable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.SEARCHABLE_ATTRIBUTE).getValue();
      Boolean projectable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.PROJECTABLE_ATTRIBUTE).getValue();
      Boolean aggregable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.AGGREGABLE_ATTRIBUTE).getValue();
      Boolean sortable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.SORTABLE_ATTRIBUTE).getValue();

      return new FieldMapping(name, searchable, projectable, aggregable, sortable,
            null, null, indexNullAs, fieldDescriptor);
   }

   private static FieldMapping keyword(FieldDescriptor fieldDescriptor, AnnotationElement.Annotation fieldAnnotation) {
      String name = name(fieldDescriptor, fieldAnnotation);
      String indexNullAs = indexNullAs(fieldAnnotation);

      Boolean searchable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.SEARCHABLE_ATTRIBUTE).getValue();
      Boolean projectable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.PROJECTABLE_ATTRIBUTE).getValue();
      Boolean aggregable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.AGGREGABLE_ATTRIBUTE).getValue();
      Boolean sortable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.SORTABLE_ATTRIBUTE).getValue();

      String normalizer = (String) fieldAnnotation.getAttributeValue(InfinispanAnnotations.NORMALIZER_ATTRIBUTE).getValue();
      if ("".equals(normalizer)) {
         normalizer = null;
      }
      Boolean norms = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.NORMS_ATTRIBUTE).getValue();

      return new FieldMapping(name, searchable, projectable, aggregable, sortable,
            null, normalizer, indexNullAs, norms, null, null, null, null, null, fieldDescriptor);
   }

   private static FieldMapping text(FieldDescriptor fieldDescriptor, AnnotationElement.Annotation fieldAnnotation) {
      String name = name(fieldDescriptor, fieldAnnotation);

      Boolean searchable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.SEARCHABLE_ATTRIBUTE).getValue();
      Boolean projectable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.PROJECTABLE_ATTRIBUTE).getValue();

      String analyzer = (String) fieldAnnotation.getAttributeValue(InfinispanAnnotations.ANALYZER_ATTRIBUTE).getValue();
      String searchAnalyzer = (String) fieldAnnotation.getAttributeValue(InfinispanAnnotations.SEARCH_ANALYZER_ATTRIBUTE).getValue();
      if ("".equals(searchAnalyzer)) {
         searchAnalyzer = null;
      }
      Boolean norms = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.NORMS_ATTRIBUTE).getValue();

      TermVector termVector = InfinispanAnnotations
            .termVector((String) fieldAnnotation.getAttributeValue(InfinispanAnnotations.TERM_VECTOR_ATTRIBUTE).getValue());

      return new FieldMapping(name, searchable, projectable, false, false,
            analyzer, null, null, norms, searchAnalyzer, termVector, null, null, null, fieldDescriptor);
   }

   private static FieldMapping decimal(FieldDescriptor fieldDescriptor, AnnotationElement.Annotation fieldAnnotation) {
      String name = name(fieldDescriptor, fieldAnnotation);
      String indexNullAs = indexNullAs(fieldAnnotation);

      Boolean searchable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.SEARCHABLE_ATTRIBUTE).getValue();
      Boolean projectable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.PROJECTABLE_ATTRIBUTE).getValue();
      Boolean aggregable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.AGGREGABLE_ATTRIBUTE).getValue();
      Boolean sortable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.SORTABLE_ATTRIBUTE).getValue();

      Integer decimalScale = (Integer) fieldAnnotation.getAttributeValue(InfinispanAnnotations.DECIMAL_SCALE_ATTRIBUTE).getValue();

      return new FieldMapping(name, searchable, projectable, aggregable, sortable,
            null, null, indexNullAs, null, null, null, decimalScale, null, null, fieldDescriptor);
   }

   private static FieldMapping embedded(FieldDescriptor fieldDescriptor, AnnotationElement.Annotation fieldAnnotation) {
      String name = name(fieldDescriptor, fieldAnnotation);

      Integer includeDepth = (Integer) fieldAnnotation.getAttributeValue(InfinispanAnnotations.INCLUDE_DEPTH_ATTRIBUTE).getValue();

      Structure structure = InfinispanAnnotations
            .structure((String) fieldAnnotation.getAttributeValue(InfinispanAnnotations.STRUCTURE_ATTRIBUTE).getValue());

      return new FieldMapping(name, true, false, false, false,
            null, null, null, null, null, null, null, includeDepth, structure, fieldDescriptor);
   }

   private static String name(FieldDescriptor fieldDescriptor, AnnotationElement.Annotation fieldAnnotation) {
      String name = (String) fieldAnnotation.getAttributeValue(InfinispanAnnotations.NAME_ATTRIBUTE).getValue();
      if (name == null) {
         name = fieldDescriptor.getName();
      }
      return name;
   }

   private static String indexNullAs(AnnotationElement.Annotation fieldAnnotation) {
      String indexNullAs = (String) fieldAnnotation.getAttributeValue(InfinispanAnnotations.INDEX_NULL_AS_ATTRIBUTE).getValue();
      if (Values.DO_NOT_INDEX_NULL.equals(indexNullAs)) {
         indexNullAs = null;
      }
      return indexNullAs;
   }
}
