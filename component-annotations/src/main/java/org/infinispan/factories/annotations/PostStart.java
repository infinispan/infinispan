package org.infinispan.factories.annotations;

import static java.lang.annotation.ElementType.METHOD;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Behaves exactly as {@link Start}.
 *
 * @author Tristan Tarrant
 * @since 9.2
 * @deprecated Since 9.4, please use {@link Start} instead.
 */
@Target(METHOD)
@Retention(RetentionPolicy.CLASS)
@Deprecated
public @interface PostStart {
   /**
    * @see Start#priority()
    */
   int priority() default 10;
}
