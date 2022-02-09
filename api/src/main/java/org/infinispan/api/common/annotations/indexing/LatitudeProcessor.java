package org.infinispan.api.common.annotations.indexing;

import org.hibernate.search.mapper.pojo.bridge.builtin.programmatic.GeoPointBinder;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingStep;
import org.infinispan.api.annotations.indexing.Latitude;

public class LatitudeProcessor implements PropertyMappingAnnotationProcessor<Latitude> {

   @Override
   public void process(PropertyMappingStep mapping, Latitude annotation,
                       PropertyMappingAnnotationProcessorContext context) {
      mapping.marker(GeoPointBinder.latitude().markerSet(annotation.marker()));
   }
}
