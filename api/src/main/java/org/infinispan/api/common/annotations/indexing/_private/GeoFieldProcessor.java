package org.infinispan.api.common.annotations.indexing._private;

import org.hibernate.search.mapper.pojo.bridge.builtin.programmatic.GeoPointBinder;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingStep;
import org.infinispan.api.annotations.indexing.GeoField;

public class GeoFieldProcessor implements PropertyMappingAnnotationProcessor<GeoField> {

   @Override
   public void process(PropertyMappingStep mapping, GeoField annotation,
                       PropertyMappingAnnotationProcessorContext context) {
      mapping.binder(createBinder(annotation));
   }

   private GeoPointBinder createBinder(GeoField annotation) {
      return GeoPointBinder.create()
            .fieldName(annotation.fieldName())
            .markerSet(annotation.marker())
            .projectable(Options.projectable(annotation.projectable()))
            .sortable(Options.sortable(annotation.sortable()));
   }
}
