package org.infinispan.commons.util;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

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
