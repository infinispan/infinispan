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
   /**
    * Optional parameter which defines the order in which this method will be called when a component has more than
    * one method annotated with {@link Stop}.  Defaults to 10.
    *
    * <p>A component's stop methods usually run only when the component registry is stopped,
    * and the component stop order is the reverse of their start order.</p>
    * <p>Stop methods defined (and annotated) in superclasses will run after the stop methods defined in derived classes.</p>
    *
    * <p>Note: Prior to 9.4, priority parameter allowed the stop methods of one component to run before or after
    * the stop methods of another component.
    * Since 9.4, the priority parameter is ignored unless the component has multiple stop methods.</p>
    *
    * @since 4.0
    * @deprecated Since 10.0, will be removed in a future version.
    */
   @Deprecated
   int priority() default 10;
}
