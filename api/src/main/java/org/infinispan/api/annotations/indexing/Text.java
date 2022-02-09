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
import org.infinispan.api.annotations.indexing.option.Norms;
import org.infinispan.api.annotations.indexing.option.Projectable;
import org.infinispan.api.annotations.indexing.option.Searchable;
import org.infinispan.api.annotations.indexing.option.TermVector;
import org.infinispan.api.common.annotations.indexing.TextProcessor;

/**
 * Maps a property to a full-text field in the index, potentially holding multiple tokens (words) of text.
 * <p>
 * Note that this annotation only creates tokenized (multi-word) text fields.
 * As a result:
 * <ul>
 *     <li>The field value must be of type String</li>
 *     <li>You must assign an analyzer when using this annotation</li>
 *     <li>This annotation does not allow making the field sortable (analyzed fields cannot be sorted on)</li>
 *     <li>This annotation does not allow making the field aggregable (analyzed fields cannot be aggregated on)</li>
 * </ul>
 * <p>
 * If you want to index a non-String value, use the {@link Basic} annotation instead.
 * If you want to index a String value, but don't want the field to be analyzed, or want it to be sortable,
 * use the {@link Keyword} annotation instead.
 * <p>
 * Simplified version for Infinispan of {@link org.hibernate.search.mapper.pojo.mapping.definition.annotation.FullTextField}
 *
 * @since 14.0
 */
@Documented
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Text.List.class)
@PropertyMapping(processor = @PropertyMappingAnnotationProcessorRef(type = TextProcessor.class, retrieval = BeanRetrieval.CONSTRUCTOR))
public @interface Text {

   /**
    * @return The name of the index field.
    */
   String name() default "";

   /**
    * @return A reference to the analyzer to use for this field.
    */
   String analyzer() default Values.DEFAULT_ANALYZER;

   /**
    * @return A reference to a different analyzer, overriding the {@link #analyzer()},
    * to use for query parameters at search time.
    * If not defined, the same {@link #analyzer()} will be used.
    */
   String searchAnalyzer() default "";

   /**
    * @return Whether index-time scoring information should be stored or not.
    * @see Norms
    */
   Norms norms() default Norms.YES;

   /**
    * @return The term vector storing strategy.
    * @see TermVector
    */
   TermVector termVector() default TermVector.NO;

   /**
    * @return Whether projections are enabled for this field.
    * @see Basic#projectable()
    * @see Projectable
    */
   Projectable projectable() default Projectable.NO;

   /**
    * @return Whether this field should be searchable.
    * @see Basic#searchable()
    * @see Searchable
    */
   Searchable searchable() default Searchable.YES;

   @Documented
   @Target({ElementType.METHOD, ElementType.FIELD})
   @Retention(RetentionPolicy.RUNTIME)
   @interface List {
      Text[] value();
   }
}
