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
import org.infinispan.api.annotations.indexing.option.VectorSimilarity;
import org.infinispan.api.common.annotations.indexing._private.VectorProcessor;

@Documented
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Vector.List.class)
@PropertyMapping(processor = @PropertyMappingAnnotationProcessorRef(type = VectorProcessor.class, retrieval = BeanRetrieval.CONSTRUCTOR))
public @interface Vector {

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
    * @return Whether this field should be searchable.
    * @see Basic#searchable()
    */
   boolean searchable() default true;

   /**
    * @return A value used instead of null values when indexing.
    * @see Basic #indexNullAs()
    */
   String indexNullAs() default Values.DO_NOT_INDEX_NULL;

   /**
    * @return The dimension of the vector.
    */
   int dimension();

   /**
    * @return The similarity algorithm.
    */
   VectorSimilarity similarity() default VectorSimilarity.L2;

   /**
    * @return The beam width used for kNN accuracy.
    */
   int beamWidth() default Values.DEFAULT_BEAN_WIDTH;

   /**
    * @return HNSW neighbor nodes.
    */
   int maxConnections() default Values.DEFAULT_MAX_CONNECTIONS;

   @Documented
   @Target({ElementType.METHOD, ElementType.FIELD})
   @Retention(RetentionPolicy.RUNTIME)
   @interface List {
      Vector[] value();
   }
}
