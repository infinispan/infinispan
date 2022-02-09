package org.infinispan.api.common.annotations.indexing;

import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.TypeMappingIndexedStep;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.TypeMappingStep;
import org.infinispan.api.annotations.indexing.Indexed;

public class IndexedProcessor implements TypeMappingAnnotationProcessor<Indexed> {

   @Override
   public void process(TypeMappingStep mapping, Indexed annotation, TypeMappingAnnotationProcessorContext context) {
      TypeMappingIndexedStep indexed = mapping.indexed();

      String indexName = annotation.index();
      if (!indexName.isEmpty()) {
         indexed.index(indexName);
      }

      indexed.enabled(annotation.enabled());
   }
}
