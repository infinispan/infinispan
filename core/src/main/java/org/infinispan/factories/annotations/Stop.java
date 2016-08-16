package org.infinispan.factories.annotations;

import static java.lang.annotation.ElementType.METHOD;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Method level annotation that indicates a (no-param) method to be called on a component registered in the
 * ComponentRegistry when the cache stops.
 * <p/>
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@Target(METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Stop {
   /**
    * Optional parameter which defines the order in which this method will be called when the ComponentRegistry moves to
    * the STOPPING state.  Defaults to 10.
    *
    * @return execution priority
    * @since 4.0
    */
   public abstract int priority() default 10;
}