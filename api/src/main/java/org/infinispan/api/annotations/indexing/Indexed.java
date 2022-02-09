package org.infinispan.api.annotations.indexing;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.hibernate.search.engine.environment.bean.BeanRetrieval;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMapping;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMappingAnnotationProcessorRef;
import org.infinispan.api.annotations.indexing.impl.IndexedProcessor;

/**
 * Maps an entity type to an index.
 * <p>
 * Simplified version for Infinispan of {@link org.hibernate.search.mapper.pojo.mapping.definition.annotation.Indexed}
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

}
