package org.infinispan.api.common.annotations.indexing;

import org.hibernate.search.engine.backend.types.ObjectStructure;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingStep;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.option.Structure;

public class EmbeddedProcessor implements PropertyMappingAnnotationProcessor<Embedded> {

   @Override
   public void process(PropertyMappingStep mapping, Embedded annotation, PropertyMappingAnnotationProcessorContext context) {
      String name = annotation.name();
      if (name.isEmpty()) {
         name = null;
      }

      mapping.indexedEmbedded(name)
            .structure((annotation.structure() == Structure.NESTED) ? ObjectStructure.NESTED : ObjectStructure.FLATTENED)
            .includeDepth(annotation.includeDepth());
   }
}
