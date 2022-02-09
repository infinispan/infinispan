package org.infinispan.api.common.annotations.indexing;

import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingGenericFieldOptionsStep;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingStep;
import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.model.Values;

public class BasicProcessor implements PropertyMappingAnnotationProcessor<Basic> {

   @Override
   public void process(PropertyMappingStep mapping, Basic annotation, PropertyMappingAnnotationProcessorContext context) {
      String name = annotation.name();
      PropertyMappingGenericFieldOptionsStep genericField = (name.isEmpty()) ?
            mapping.genericField() : mapping.genericField(name);

      genericField.sortable(Options.sortable(annotation.sortable()));
      genericField.aggregable(Options.aggregable(annotation.aggregable()));

      String indexNullAs = annotation.indexNullAs();
      if (indexNullAs != null && !Values.DO_NOT_INDEX_NULL.equals(indexNullAs)) {
         genericField.indexNullAs(indexNullAs);
      }

      genericField.projectable(Options.projectable(annotation.projectable()));
      genericField.searchable(Options.searchable(annotation.searchable()));
   }
}
