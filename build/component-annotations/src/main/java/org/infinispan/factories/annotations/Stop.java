package org.infinispan.factories.annotations;

import static java.lang.annotation.ElementType.METHOD;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Method level annotation that indicates a (no-param) method to be called on a component registered in the
 * component registry when the registry stops.
 * <p/>
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@Target(METHOD)
@Retention(RetentionPolicy.CLASS)
public @interface Stop {
}
