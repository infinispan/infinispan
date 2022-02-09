package org.infinispan.api.annotations.indexing.impl;

import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingStep;
import org.infinispan.api.annotations.indexing.Id;

public class IdProcessor implements PropertyMappingAnnotationProcessor<Id> {

   @Override
   public void process(PropertyMappingStep mapping, Id annotation, PropertyMappingAnnotationProcessorContext context) {
      mapping.documentId();
   }
}
