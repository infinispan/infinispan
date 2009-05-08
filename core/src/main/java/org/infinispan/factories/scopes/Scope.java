package org.infinispan.factories.scopes;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Defines the scope of a component in a cache system.  If not specified, components default to the {@link
 * Scopes#NAMED_CACHE} scope.
 *
 * @author Manik Surtani
 * @see Scopes
 * @since 4.0
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Scope {
   Scopes value();
}
