package org.infinispan.factories.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used for components that will be registered in the {@link org.infinispan.factories.ComponentRegistry},
 * that are meant to be retained in the component registry even after the component registry is stopped.  Components
 * annotated as such would not have to be recreated and rewired between stopping and starting a component registry.
 * <br />
 * As a rule of thumb though, use of this annotation on components should be avoided, since resilient components are
 * retained even after a component registry is stopped and consume resources.  Only components necessary for and critical
 * to bootstrapping and restarting the component registry should be annotated as such.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
// ensure this annotation is available at runtime.
@Retention(RetentionPolicy.RUNTIME)

// only applies to classes.
@Target(ElementType.TYPE)
public @interface SurvivesRestarts {
}
