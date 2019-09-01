package org.infinispan.factories.scopes;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Defines the scope of a component in a cache system.  If not specified, components default to the {@link
 * Scopes#NAMED_CACHE} scope.
 *
 * <p>Note: The {@code Scope} annotation is inherited so that the annotation processor can find subclasses.
 * Although annotating interfaces is allowed, it is preferable to annotate only classes.</p>
 *
 * @author Manik Surtani
 * @see Scopes
 * @since 4.0
 */
@Retention(RetentionPolicy.CLASS)
@Inherited
public @interface Scope {
   Scopes value();
}
