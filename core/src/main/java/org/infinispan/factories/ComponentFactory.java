package org.infinispan.factories;

import org.infinispan.factories.impl.ComponentAlias;

/**
 * Factory for Infinispan components.
 *
 * <p>Implementations should usually be annotated with {@link org.infinispan.factories.annotations.DefaultFactoryFor}
 * and {@link org.infinispan.factories.scopes.Scope} (the factory must have the same scope as the components it creates).</p>
 *
 * @since 9.4
 */
public interface ComponentFactory {
   /**
    * @return Either a component instance or a {@link ComponentAlias} pointing to another component.
    */
   Object construct(String componentName);
}
