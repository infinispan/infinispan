package org.infinispan.configuration.parsing;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Namespaces. An annotation which allows specifying multiple {@link Namespace}s recognized by an implementation
 * of a {@link ConfigurationParser}
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Namespaces {
   Namespace[] value();
}
