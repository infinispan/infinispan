package org.infinispan.commons.configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * BuiltBy. An annotation for configuration beans to specify what builder builds them.
 * This annotation is required on all non-core configuration classes (i.e. ones which reside
 * in external modules)
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface BuiltBy {
   @SuppressWarnings("rawtypes")
   Class<? extends Builder> value();
}
