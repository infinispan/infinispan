package org.infinispan.api.annotations.indexing;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.hibernate.search.engine.environment.bean.BeanRetrieval;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMapping;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMappingAnnotationProcessorRef;
import org.infinispan.api.annotations.indexing.model.LatLng;
import org.infinispan.api.common.annotations.indexing._private.GeoPointProcessor;

/**
 * Defines a {@link LatLng} binding from a type or a property
 * to a {@link LatLng} field representing a point on earth.
 * <p>
 * If the longitude and latitude information is hosted on two different properties,
 * {@code @GeoPoint} must be used on the entity (class level).
 * The {@link Latitude} and {@link Longitude} annotations must mark the properties.
 * <pre><code>
 * &#064;GeoPoint(fieldName="home")
 * public class User {
 *     &#064;Latitude(fieldName="home")
 *     public Double getHomeLatitude() { ... }
 *     &#064;Longitude(fieldName="home")
 *     public Double getHomeLongitude() { ... }
 * }
 * </code></pre>
 *
 * More geo fields can be defined, using multiple {@link GeoPoint} annotations
 * having different field names.
 *
 * @since 15.1
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.TYPE})
@Documented
@Repeatable(GeoPoint.GeoPoints.class)
@TypeMapping(processor = @TypeMappingAnnotationProcessorRef(type = GeoPointProcessor.class, retrieval = BeanRetrieval.CONSTRUCTOR))
public @interface GeoPoint {

   /**
    * The name of the index field holding spatial information.
    * <p>
    * the name must be provided
    *
    * @return the field name
    */
   String fieldName() default "";

   /**
    * @return Whether projections are enabled for this field.
    * @see Basic#projectable()
    */
   boolean projectable() default false;

   /**
    * @return Whether this field should be sortable.
    * @see Basic#sortable()
    */
   boolean sortable() default false;

   @Retention(RetentionPolicy.RUNTIME)
   @Target({ElementType.METHOD, ElementType.FIELD, ElementType.TYPE})
   @Documented
   @interface GeoPoints {
      GeoPoint[] value();
   }
}
