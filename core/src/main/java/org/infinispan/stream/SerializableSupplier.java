package org.infinispan.stream;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * This is a simple Supplier that is also Serializable.  This is useful to not require users to cast their Supplier
 * when a Serializable version is required.  This allows lambdas to be placed directly for easy convenience.
 * @param <T> The type returned from the supplier
 */
@FunctionalInterface
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {

}
