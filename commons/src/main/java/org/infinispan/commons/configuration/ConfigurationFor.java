package org.infinispan.commons.configuration;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * ConfigurationFor. Indicates the class that this object is a configuration for
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ConfigurationFor {
   Class<?> value();
}
