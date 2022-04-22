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
import org.infinispan.api.common.annotations.indexing._private.BasicProcessor;

/**
 * Maps an entity property to a field in the index.
 * <p>
 * This is a generic annotation that will work for any standard field type:
 * {@link String}, {@link Integer}, {@link java.time.LocalDate}, ...
 * <p>
 * Note that this annotation, being generic, does not offer configuration options
 * that are specific to only some types of fields.
 * Use more specific annotations if you want that kind of configuration.
 * For example, to define a tokenized (multi-word) text field, use {@link Text}.
 * To define a non-tokenized (single-word), but normalized (lowercased, ...) text field, use {@link Keyword}.
 * <p>
 * Simplified version for Infinispan of {@link org.hibernate.search.mapper.pojo.mapping.definition.annotation.GenericField}
 *
 * @since 14.0
 */
@Documented
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Basic.List.class)
@PropertyMapping(processor = @PropertyMappingAnnotationProcessorRef(type = BasicProcessor.class, retrieval = BeanRetrieval.CONSTRUCTOR))
public @interface Basic {

   /**
    * @return The name of the index field.
    */
   String name() default "";

   /**
    * Whether we want to be able to obtain the value of the field as a projection.
    * <p>
    * This usually means that the field will be stored in the index, but it is more subtle than that, for instance in the
    * case of projection by distance.
    *
    * @return Whether projections are enabled for this field.
    */
   boolean projectable() default false;

   /**
    * Whether a field can be used in sorts.
    *
    * @return Whether this field should be sortable.
    */
   boolean sortable() default false;

   /**
    * Whether we want to be able to search the document using this field.
    * <p>
    * If the field is not searchable, search predicates cannot be applied to it.
    *
    * @return Whether this field should be searchable.
    */
   boolean searchable() default true;

   /**
    * Whether the field can be used in aggregations.
    * <p>
    * This usually means that the field will have doc-values stored in the index.
    *
    * @return Whether aggregations are enabled for this field.
    */
   boolean aggregable() default false;

   /**
    * @return A value used instead of null values when indexing.
    */
   String indexNullAs() default Values.DO_NOT_INDEX_NULL;

   @Documented
   @Target({ElementType.METHOD, ElementType.FIELD})
   @Retention(RetentionPolicy.RUNTIME)
   @interface List {
      Basic[] value();
   }
}
