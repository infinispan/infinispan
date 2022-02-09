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
import org.infinispan.api.annotations.indexing.model.Values;
import org.infinispan.api.annotations.indexing.option.Structure;
import org.infinispan.api.common.annotations.indexing.EmbeddedProcessor;

/**
 * Maps a property to an object field whose fields are the same as those defined in the property type.
 * <p>
 * This allows search queries on a single index to use data from multiple entities.
 * <p>
 * To get the original structure, the {@link Structure#NESTED nested structure} must be used,
 * but this has an impact on performance and how queries must be structured.
 * <p>
 * Simplified version for Infinispan of {@link org.hibernate.search.mapper.pojo.mapping.definition.annotation.IndexedEmbedded}
 *
 * @since 14.0
 */
@Documented
@Repeatable(Embedded.List.class)
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@PropertyMapping(processor = @PropertyMappingAnnotationProcessorRef(type = EmbeddedProcessor.class, retrieval = BeanRetrieval.CONSTRUCTOR))
public @interface Embedded {

   /**
    * @return The name of the object field created to represent this {@code @Embedded}.
    * Defaults to the property name.
    */
   String name() default "";

   /**
    * The number of levels of indexed-embedded that will have all their fields included by default.
    * <p>
    * {@code includeDepth} is the number of `@Embedded` that will be traversed
    * and for which all fields of the indexed-embedded element will be included:
    * <ul>
    * <li>{@code includeDepth=0} means fields of the indexed-embedded element are <strong>not</strong> included,
    * nor is any field of nested indexed-embedded elements.
    * <li>{@code includeDepth=1} means fields of the indexed-embedded element <strong>are</strong> included,
    * but <strong>not</strong> fields of nested indexed-embedded elements.
    * <li>And so on.
    * </ul>
    * The default is {@link Values#DEFAULT_EMBEDDED_INCLUDE_DEPTH}
    *
    * @return The number of levels of indexed-embedded that will have all their fields included by default.
    */
   int includeDepth() default Values.DEFAULT_EMBEDDED_INCLUDE_DEPTH;

   /**
    * @return How the structure of the object field created for this indexed-embedded
    * is preserved upon indexing.
    * @see Structure
    */
   Structure structure() default Structure.NESTED;

   @Documented
   @Target({ElementType.METHOD, ElementType.FIELD})
   @Retention(RetentionPolicy.RUNTIME)
   @interface List {
      Embedded[] value();
   }
}
