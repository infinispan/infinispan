package org.infinispan.commons.configuration;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * BuiltBy. An annotation for configuration beans to specify what builder builds them.
 * This annotation is required on all non-core configuration classes (i.e. ones which reside
 * in external modules)
 *
 * @author Tristan Tarrant
 * @since 5.2
 * @public
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface BuiltBy {
   @SuppressWarnings("rawtypes")
   Class<? extends Builder> value();
}
