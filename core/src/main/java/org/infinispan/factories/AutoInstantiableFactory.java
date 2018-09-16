package org.infinispan.factories;

/**
 * Component factories that implement this interface can be instantiated automatically by component registries when
 * looking up components.  Typically, most component factories will implement this.  One known exception is the {@link
 * org.infinispan.factories.BootstrapFactory}.
 * <p>
 * Anything implementing this interface should expose a public, no-arg constructor.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface AutoInstantiableFactory {
}
