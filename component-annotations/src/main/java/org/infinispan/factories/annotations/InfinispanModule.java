package org.infinispan.factories.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Mechanism for specifying the name of modules.
 *
 * There must be exactly one {@code InfinispanModule} annotation in each module.
 *
 * <p>It would have been nice to put the annotation on a package,
 * but package-info.java source files are excluded from compilation
 * because of MCOMPILER-205.</p>
 *
 * @author Dan Berindei
 * @since 10.0
 */
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.CLASS)
public @interface InfinispanModule {
   String name();

   String[] requiredModules() default {};

   String[] optionalModules() default {};
}
