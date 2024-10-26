package org.infinispan.factories;

/**
 * Component factories that implement this interface can be instantiated automatically by component registries when
 * looking up components.  Typically, most component factories will implement this.
 * <p/>
 * Anything implementing this interface should expose a public, no-arg constructor.
 * <p/>
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface AutoInstantiableFactory {
}
