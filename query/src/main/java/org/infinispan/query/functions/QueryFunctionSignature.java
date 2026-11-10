package org.infinispan.query.functions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Documents the expected argument and return types of a {@link QueryFunction#evaluate}
 * method. This is a documentation-only convention: the runtime does not validate or
 * enforce the declared types.
 *
 * @since 16.4
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface QueryFunctionSignature {

   String STRING = "string";
   String MATH = "math";
   String DATE = "date";

   /**
    * Expected argument types, in order. Use {@code Object.class} for an argument
    * that may be of any type.
    */
   Class<?>[] args();

   /**
    * Expected return type. Defaults to {@code Object.class} when the return type
    * is not fixed.
    */
   Class<?> returns() default Object.class;

   /**
    * Functional category of the function, e.g. {@code "math"}, {@code "string"},
    * {@code "date"}, {@code "regexp"}, {@code "conversion"}.
    */
   String category() default "";
}
