package org.infinispan.api.common.annotations.indexing;

import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingKeywordFieldOptionsStep;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingStep;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.model.Values;

public class KeywordProcessor implements PropertyMappingAnnotationProcessor<Keyword> {

   @Override
   public void process(PropertyMappingStep mapping, Keyword annotation, PropertyMappingAnnotationProcessorContext context) {
      String name = annotation.name();
      PropertyMappingKeywordFieldOptionsStep keywordField = (name.isEmpty()) ?
            mapping.keywordField() : mapping.keywordField(name);

      String normalizer = annotation.normalizer();
      if (!normalizer.isEmpty()) {
         keywordField.normalizer(annotation.normalizer());
      }

      keywordField.norms(Options.norms(annotation.norms()));
      keywordField.sortable(Options.sortable(annotation.sortable()));
      keywordField.aggregable(Options.aggregable(annotation.aggregable()));

      String indexNullAs = annotation.indexNullAs();
      if (indexNullAs != null && !Values.DO_NOT_INDEX_NULL.equals(indexNullAs)) {
         keywordField.indexNullAs(indexNullAs);
      }

      keywordField.projectable(Options.projectable(annotation.projectable()));
      keywordField.searchable(Options.searchable(annotation.searchable()));
   }
}
