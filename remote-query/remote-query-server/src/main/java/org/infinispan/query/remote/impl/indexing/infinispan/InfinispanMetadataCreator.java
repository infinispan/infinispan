package org.infinispan.query.remote.impl.indexing.infinispan;

import java.util.Map;

import org.infinispan.api.annotations.indexing.model.Values;
import org.infinispan.api.annotations.indexing.option.Structure;
import org.infinispan.api.annotations.indexing.option.TermVector;
import org.infinispan.api.annotations.indexing.option.VectorSimilarity;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.descriptors.AnnotationElement;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.remote.impl.indexing.FieldMapping;
import org.infinispan.query.remote.impl.logging.Log;

public final class InfinispanMetadataCreator {

   private static final Log log = LogFactory.getLog(InfinispanMetadataCreator.class, Log.class);

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

      fieldAnnotation = annotations.get(InfinispanAnnotations.VECTOR_ANNOTATION);
      if (fieldAnnotation != null) {
         return vector(fieldDescriptor, fieldAnnotation);
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

      return FieldMapping.make(fieldDescriptor, name, searchable, projectable, aggregable, sortable)
            .indexNullAs(indexNullAs)
            .build();
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

      return FieldMapping.make(fieldDescriptor, name, searchable, projectable, aggregable, sortable)
            .indexNullAs(indexNullAs)
            .keyword(normalizer, norms)
            .build();
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

      return FieldMapping.make(fieldDescriptor, name, searchable, projectable, false, false)
            .text(analyzer, searchAnalyzer, norms, termVector)
            .build();
   }

   private static FieldMapping decimal(FieldDescriptor fieldDescriptor, AnnotationElement.Annotation fieldAnnotation) {
      String name = name(fieldDescriptor, fieldAnnotation);
      String indexNullAs = indexNullAs(fieldAnnotation);

      Boolean searchable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.SEARCHABLE_ATTRIBUTE).getValue();
      Boolean projectable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.PROJECTABLE_ATTRIBUTE).getValue();
      Boolean aggregable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.AGGREGABLE_ATTRIBUTE).getValue();
      Boolean sortable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.SORTABLE_ATTRIBUTE).getValue();

      Integer decimalScale = (Integer) fieldAnnotation.getAttributeValue(InfinispanAnnotations.DECIMAL_SCALE_ATTRIBUTE).getValue();

      return FieldMapping.make(fieldDescriptor, name, searchable, projectable, aggregable, sortable)
            .indexNullAs(indexNullAs)
            .decimalScale(decimalScale)
            .build();
   }

   private static FieldMapping vector(FieldDescriptor fieldDescriptor, AnnotationElement.Annotation fieldAnnotation) {
      String name = name(fieldDescriptor, fieldAnnotation);
      String indexNullAs = indexNullAs(fieldAnnotation);

      Boolean searchable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.SEARCHABLE_ATTRIBUTE).getValue();
      Boolean projectable = (Boolean) fieldAnnotation.getAttributeValue(InfinispanAnnotations.PROJECTABLE_ATTRIBUTE).getValue();

      Integer dimension = (Integer) fieldAnnotation.getAttributeValue(InfinispanAnnotations.DIMENSION_ATTRIBUTE).getValue();
      if (dimension == null) {
         throw log.dimensionAttributeRequired(name);
      }
      VectorSimilarity similarity = InfinispanAnnotations
            .vectorSimilarity((String) fieldAnnotation.getAttributeValue(InfinispanAnnotations.SIMILARITY_ATTRIBUTE).getValue());
      Integer beamWidth = (Integer) fieldAnnotation.getAttributeValue(InfinispanAnnotations.BEAM_WIDTH_ATTRIBUTE).getValue();
      Integer maxConnection = (Integer) fieldAnnotation.getAttributeValue(InfinispanAnnotations.MAX_CONNECTIONS_ATTRIBUTE).getValue();

      return FieldMapping.make(fieldDescriptor, name, searchable, projectable, false, false)
            .indexNullAs(indexNullAs)
            .vector(dimension, similarity, beamWidth, maxConnection)
            .build();
   }

   private static FieldMapping embedded(FieldDescriptor fieldDescriptor, AnnotationElement.Annotation fieldAnnotation) {
      String name = name(fieldDescriptor, fieldAnnotation);

      Integer includeDepth = (Integer) fieldAnnotation.getAttributeValue(InfinispanAnnotations.INCLUDE_DEPTH_ATTRIBUTE).getValue();

      Structure structure = InfinispanAnnotations
            .structure((String) fieldAnnotation.getAttributeValue(InfinispanAnnotations.STRUCTURE_ATTRIBUTE).getValue());

      return FieldMapping.make(fieldDescriptor, name, true, false, false, false)
            .embedded(includeDepth, structure)
            .build();
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
