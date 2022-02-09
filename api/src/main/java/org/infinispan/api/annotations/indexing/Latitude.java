package org.infinispan.api.annotations.indexing;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.hibernate.search.engine.environment.bean.BeanRetrieval;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMapping;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorRef;
import org.infinispan.api.common.annotations.indexing.LatitudeProcessor;

/**
 * Mark the property hosting the latitude of a specific spatial coordinate.
 * The property must be of type {@code Double} or {@code double}.
 * <p>
 * Infinispan version of {@link org.hibernate.search.mapper.pojo.bridge.builtin.annotation.Latitude}
 *
 * @since 14.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
@Documented
@PropertyMapping(processor = @PropertyMappingAnnotationProcessorRef(type = LatitudeProcessor.class, retrieval = BeanRetrieval.CONSTRUCTOR))
public @interface Latitude {

   /**
    * @return The name of the marker this marker belongs to.
    * Set it to the value of {@link GeoCoordinates#marker()}
    * so that the bridge detects this marker.
    */
   String marker() default "";

}
