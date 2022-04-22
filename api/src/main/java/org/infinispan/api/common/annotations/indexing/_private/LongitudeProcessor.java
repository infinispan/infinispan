package org.infinispan.api.common.annotations.indexing._private;

import org.hibernate.search.mapper.pojo.bridge.builtin.programmatic.GeoPointBinder;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingStep;
import org.infinispan.api.annotations.indexing.Longitude;

public class LongitudeProcessor implements PropertyMappingAnnotationProcessor<Longitude> {

   @Override
   public void process(PropertyMappingStep mapping, Longitude annotation,
                       PropertyMappingAnnotationProcessorContext context) {
      mapping.marker(GeoPointBinder.longitude().markerSet(annotation.marker()));
   }
}
