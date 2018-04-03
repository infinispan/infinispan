package org.infinispan.factories.annotations;

import static java.lang.annotation.ElementType.TYPE;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that is used internally, for defining a DEFAULT factory to be used when constructing components.  This
 * annotation allows you to define which components can be constructed by the annotated factory.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@Target(TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DefaultFactoryFor {
   /**
    * Components that may be constructed by a factory annotated with this annotation.
    *
    * @return classes that can be constructed by this factory
    */
   Class<?>[] classes() default {};

   String[] names() default {};
}
