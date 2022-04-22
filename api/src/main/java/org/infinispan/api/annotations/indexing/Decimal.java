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
import org.infinispan.api.common.annotations.indexing._private.DecimalProcessor;

/**
 * Maps a property to a scaled number field in the index,
 * i.e. a numeric field for integer or floating-point values
 * that require a higher precision than doubles
 * but always have roughly the same scale.
 * <p>
 * Useful for {@link java.math.BigDecimal} and {@link java.math.BigInteger} in particular.
 * <p>
 * Simplified version for Infinispan of {@link org.hibernate.search.mapper.pojo.mapping.definition.annotation.ScaledNumberField}
 *
 * @see #decimalScale()
 *
 * @since 14.0
 */
@Documented
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Decimal.List.class)
@PropertyMapping(processor = @PropertyMappingAnnotationProcessorRef(type = DecimalProcessor.class, retrieval = BeanRetrieval.CONSTRUCTOR))
public @interface Decimal {

   /**
    * @return The name of the index field.
    */
   String name() default "";

   /**
    * @return How the scale of values should be adjusted before indexing as a fixed-precision integer.
    * A positive {@code decimalScale} will shift the decimal point to the right before rounding to the nearest integer and indexing,
    * effectively retaining that many digits after the decimal place in the index.
    * Since numbers are indexed with a fixed number of bits,
    * this increase in precision also means that the maximum value that can be indexed will be smaller.
    * A negative {@code decimalScale} will shift the decimal point to the left before rounding to the nearest integer and indexing,
    * effectively setting that many of the smaller digits to zero in the index.
    * Since numbers are indexed with a fixed number of bits,
    * this decrease in precision also means that the maximum value that can be indexed will be larger.
    * By default, two decimals will be used.
    */
   int decimalScale() default 2;

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
    * @see Basic#indexNullAs()
    */
   String indexNullAs() default Values.DO_NOT_INDEX_NULL;

   @Documented
   @Target({ElementType.METHOD, ElementType.FIELD})
   @Retention(RetentionPolicy.RUNTIME)
   @interface List {
      Decimal[] value();
   }
}
