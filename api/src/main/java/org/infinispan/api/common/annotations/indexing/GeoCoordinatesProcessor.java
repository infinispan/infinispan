package org.infinispan.api.common.annotations.indexing;

import org.hibernate.search.mapper.pojo.bridge.builtin.programmatic.GeoPointBinder;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingStep;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.TypeMappingStep;
import org.infinispan.api.annotations.indexing.GeoCoordinates;

public class GeoCoordinatesProcessor implements TypeMappingAnnotationProcessor<GeoCoordinates>,
      PropertyMappingAnnotationProcessor<GeoCoordinates> {

   @Override
   public void process(PropertyMappingStep mapping, GeoCoordinates annotation,
                       PropertyMappingAnnotationProcessorContext context) {
      mapping.binder(createBinder(annotation));
   }

   @Override
   public void process(TypeMappingStep mapping, GeoCoordinates annotation,
                       TypeMappingAnnotationProcessorContext context) {
      mapping.binder(createBinder(annotation));
   }

   private GeoPointBinder createBinder(GeoCoordinates annotation) {
      return GeoPointBinder.create()
            .fieldName(annotation.fieldName())
            .markerSet(annotation.marker())
            .projectable(Options.projectable(annotation.projectable()))
            .sortable(Options.sortable(annotation.sortable()));
   }
}
