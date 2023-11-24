package org.infinispan.api.common.annotations.indexing._private;

import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingStep;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingVectorFieldOptionsStep;
import org.infinispan.api.annotations.indexing.Vector;
import org.infinispan.api.annotations.indexing.model.Values;

public class VectorProcessor implements PropertyMappingAnnotationProcessor<Vector> {

   @Override
   public void process(PropertyMappingStep mapping, Vector annotation, PropertyMappingAnnotationProcessorContext context) {
      String name = annotation.name();
      PropertyMappingVectorFieldOptionsStep vectorField = (name.isEmpty()) ?
            mapping.vectorField(annotation.dimension()) : mapping.vectorField(annotation.dimension(), name);

      vectorField.searchable(Options.searchable(annotation.searchable()));
      vectorField.projectable(Options.projectable(annotation.projectable()));

      String indexNullAs = annotation.indexNullAs();
      if (indexNullAs != null && !Values.DO_NOT_INDEX_NULL.equals(indexNullAs)) {
         vectorField.indexNullAs(indexNullAs);
      }

      vectorField.vectorSimilarity(Options.vectorSimilarity(annotation.similarity()));
      vectorField.beamWidth(annotation.beamWidth());
      vectorField.maxConnections(annotation.maxConnections());
   }
}
