package org.infinispan.commons.configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines the configuration used to configure the given class instances
 *
 * @author wburns
 * @since 7.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ConfiguredBy {
   Class<?> value();
}
