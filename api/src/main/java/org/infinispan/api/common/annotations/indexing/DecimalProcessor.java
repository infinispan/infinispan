package org.infinispan.api.common.annotations.indexing;

import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingScaledNumberFieldOptionsStep;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingStep;
import org.infinispan.api.annotations.indexing.Decimal;
import org.infinispan.api.annotations.indexing.model.Values;

public class DecimalProcessor implements PropertyMappingAnnotationProcessor<Decimal>  {

   @Override
   public void process(PropertyMappingStep mapping, Decimal annotation, PropertyMappingAnnotationProcessorContext context) {
      String name = annotation.name();
      PropertyMappingScaledNumberFieldOptionsStep scaledNumberField = (name.isEmpty()) ?
            mapping.scaledNumberField() : mapping.scaledNumberField(name);

      scaledNumberField.decimalScale(annotation.decimalScale());

      scaledNumberField.sortable(Options.sortable(annotation.sortable()));
      scaledNumberField.aggregable(Options.aggregable(annotation.aggregable()));

      String indexNullAs = annotation.indexNullAs();
      if (indexNullAs != null && !Values.DO_NOT_INDEX_NULL.equals(indexNullAs)) {
         scaledNumberField.indexNullAs(indexNullAs);
      }

      scaledNumberField.projectable(Options.projectable(annotation.projectable()));
      scaledNumberField.searchable(Options.searchable(annotation.searchable()));
   }
}
