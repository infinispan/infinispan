package org.infinispan.factories.scopes;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Defines the scope of a component in a cache system.  If not specified, components default to the {@link
 * Scopes#NAMED_CACHE} scope.
 *
 * <p>Note: The {@code Scope} annotation should be present on the implementation class, and if an interface
 * has the annotation it should have the same value.
 * In the future, annotations on interfaces and superclasses will be ignored.</p>
 *
 * @author Manik Surtani
 * @see Scopes
 * @since 4.0
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Scope {
   Scopes value();
}
