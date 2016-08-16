package org.infinispan.commons.util;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.LOCAL_VARIABLE;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PACKAGE;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * An experimental user-facing API. Elements annotated with this annotation
 * are experimental and may get removed from the distribution at any time.
 *
 * @since 8.0
 */
@Retention(RUNTIME)
@Target({TYPE, METHOD, FIELD, PACKAGE, CONSTRUCTOR, LOCAL_VARIABLE, PARAMETER})
public @interface Experimental {
   String comment() default "";
}
