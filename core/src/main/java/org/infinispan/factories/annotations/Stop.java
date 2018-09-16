package org.infinispan.factories.annotations;

import static java.lang.annotation.ElementType.METHOD;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Method level annotation that indicates a (no-param) method to be called on a component registered in the
 * component registry when the registry stops.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@Target(METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Stop {
   /**
    * Optional parameter which defines the order in which this method will be called when a component has more than
    * one method annotated with {@link Start}.  Defaults to 10.
    *
    * <p>Note: Prior to 9.4, priority parameter allowed the stop methods of one component to run before or after
    * the stop methods of another component.
    * Since 9.4, the priority parameter is ignored unless the component has multiple stop methods.
    * A component's stop methods will always run after the stop methods of its dependencies during start.
    * Changes made to the dependency graph after the component started (with
    * {@link org.infinispan.factories.impl.BasicComponentRegistry#replaceComponent(String, Object, boolean)}
    * or {@link org.infinispan.factories.impl.BasicComponentRegistry#addDynamicDependency(String, String)})
    * are also ignored.</p>
    *
    * @since 4.0
    */
   int priority() default 10;
}
