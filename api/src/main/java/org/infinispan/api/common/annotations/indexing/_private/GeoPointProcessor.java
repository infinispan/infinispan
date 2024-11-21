package org.infinispan.api.common.annotations.indexing._private;

import org.hibernate.search.mapper.pojo.bridge.builtin.programmatic.GeoPointBinder;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.TypeMappingStep;
import org.infinispan.api.annotations.indexing.GeoPoint;

public class GeoPointProcessor implements TypeMappingAnnotationProcessor<GeoPoint> {

   @Override
   public void process(TypeMappingStep mapping, GeoPoint annotation,
                       TypeMappingAnnotationProcessorContext context) {
      mapping.binder(createBinder(annotation));
   }

   private GeoPointBinder createBinder(GeoPoint annotation) {
      return GeoPointBinder.create()
            .fieldName(annotation.fieldName())
            // in Infinispan we use the field name as marker set,
            // this is mandatory if we have more @GeoPoint defined on the same entity
            .markerSet(annotation.fieldName())
            .projectable(Options.projectable(annotation.projectable()))
            .sortable(Options.sortable(annotation.sortable()));
   }
}
