package org.infinispan.config;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that marks fields in {@link Configuration} as being modifiable even after the cache has started.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 4.0
 */

// ensure this annotation is available at runtime.
@Retention(RetentionPolicy.RUNTIME)

// ensure that this annotation is documented on fields in Configuration
@Documented

// only applies to fields.
@Target(ElementType.FIELD)

public @interface Dynamic {
}
