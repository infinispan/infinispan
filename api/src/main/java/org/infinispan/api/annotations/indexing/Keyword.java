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
import org.infinispan.api.common.annotations.indexing._private.KeywordProcessor;

/**
 * Maps a property to a keyword field in the index, holding a single token (word) of text.
 * <p>
 * On contrary to {@link Text}, this annotation only creates non-tokenized (single-word) text fields.
 * As a result:
 * <ul>
 *     <li>The field value must be of type String</li>
 *     <li>You cannot assign an analyzer when using this annotation</li>
 *     <li>You can, however, assign a normalizer (which is an analyzer that doesn't perform tokenization)
 *     when using this annotation</li>
 *     <li>This annotation allows to make the field sortable</li>
 * </ul>
 * <p>
 * If you want to index a non-String value, use the {@link Basic} annotation instead.
 * If you want to index a String value, but want the field to be tokenized, use {@link Text} instead.
 * <p>
 * Simplified version for Infinispan of {@link org.hibernate.search.mapper.pojo.mapping.definition.annotation.KeywordField}
 *
 * @since 14.0
 */
@Documented
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Keyword.List.class)
@PropertyMapping(processor = @PropertyMappingAnnotationProcessorRef(type = KeywordProcessor.class, retrieval = BeanRetrieval.CONSTRUCTOR))
public @interface Keyword {

   /**
    * @return The name of the index field.
    */
   String name() default "";

   /**
    * @return A reference to the normalizer to use for this field.
    * Defaults to an empty string, meaning no normalization at all.
    */
   String normalizer() default "";

   /**
    * @return Whether index time scoring information should be stored or not.
    * @see Text#norms()
    */
   boolean norms() default true;

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

   /**
    * @return Whether this field should be searchable.
    * @see Basic#searchable()
    */
   boolean searchable() default true;

   /**
    * @return Whether aggregations are enabled for this field.
    * @see Basic#aggregable()
    */
   boolean aggregable() default false;

   /**
    * @return A value used instead of null values when indexing.
    * @see Basic #indexNullAs()
    */
   String indexNullAs() default Values.DO_NOT_INDEX_NULL;

   @Documented
   @Target({ElementType.METHOD, ElementType.FIELD})
   @Retention(RetentionPolicy.RUNTIME)
   @interface List {
      Keyword[] value();
   }
}
