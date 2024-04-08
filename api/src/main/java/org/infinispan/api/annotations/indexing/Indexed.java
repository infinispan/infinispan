package org.infinispan.api.annotations.indexing;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.hibernate.search.engine.environment.bean.BeanRetrieval;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMapping;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMappingAnnotationProcessorRef;
import org.infinispan.api.common.annotations.indexing._private.IndexedProcessor;

/**
 * Maps an entity type to an index.
 * <p>
 * Simplified version for Infinispan of {@link org.hibernate.search.mapper.pojo.mapping.definition.annotation.Indexed}
 *
 * @since 14.0
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@TypeMapping(processor = @TypeMappingAnnotationProcessorRef(type = IndexedProcessor.class, retrieval = BeanRetrieval.CONSTRUCTOR))
public @interface Indexed {

   /**
    * @return The name of the index.
    * Defaults to the entity name.
    */
   String index() default "";

   /**
    * @return {@code true} to map the type to an index (the default),
    * {@code false} to disable the mapping to an index.
    * Useful to disable indexing when subclassing an indexed type.
    */
   boolean enabled() default true;

   /**
    * Enables indexes / queries by keys.
    *
    * @return The fully qualified name of the type of entities used as keys
    */
   String keyEntity() default "";

   /**
    * Used only if {@link #keyEntity()} is provided.
    *
    * @return The property name to use in the queries to indicate the key of the entry
    */
   String keyPropertyName() default "key";

   /**
    * Used only if {@link #keyEntity()} is provided.
    *
    * @return The number of levels of indexed-key that will have all their fields included by default.
    * By default, three levels will be included.
    *
    * @see Embedded#includeDepth()
    */
   int keyIncludeDepth() default 3;

}
