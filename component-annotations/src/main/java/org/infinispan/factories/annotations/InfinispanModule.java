package org.infinispan.factories.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Mechanism for specifying the name and dependencies of Infinispan modules.
 * <p>
 * There must be exactly one {@code InfinispanModule} annotation in each module, placed on an implementation of the
 * {@code org.infinispan.lifecycle.ModuleLifecycle} interface.
 *
 * <p>It would have been nice to put the annotation on a package,
 * but package-info.java source files are excluded from compilation
 * because of MCOMPILER-205.</p>
 *
 * @author Dan Berindei
 * @since 10.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.CLASS)
public @interface InfinispanModule {

   /**
    * The unique name of the module.
    */
   String name();

   /**
    * The set of required dependencies (module names).
    */
   String[] requiredModules() default {};

   /**
    * The set of optional dependencies (module names).
    */
   String[] optionalModules() default {};
}
