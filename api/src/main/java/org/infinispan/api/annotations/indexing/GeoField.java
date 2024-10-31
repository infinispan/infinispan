package org.infinispan.api.annotations.indexing;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.hibernate.search.engine.environment.bean.BeanRetrieval;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMapping;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorRef;
import org.infinispan.api.annotations.indexing.model.LatLng;
import org.infinispan.api.common.annotations.indexing._private.GeoFieldProcessor;

/**
 * Defines a {@link LatLng} binding from a type or a property
 * to a {@link LatLng} field representing a point on earth.
 * <p>
 * {@code @GeoField} can be used on a property of type {@link LatLng}:
 * <pre><code>
 * public class User {
 *     &#064;GeoField
 *     public LatLng getHome() { ... }
 * }
 * </code></pre>
 *
 * @since 15.1
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.TYPE})
@Documented
@Repeatable(GeoField.GeoFields.class)
@PropertyMapping(processor = @PropertyMappingAnnotationProcessorRef(type = GeoFieldProcessor.class, retrieval = BeanRetrieval.CONSTRUCTOR))
public @interface GeoField {

   /**
    * @return The name of the index field.
    */
   String name() default "";

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
   @interface GeoFields {
      GeoField[] value();
   }
}
