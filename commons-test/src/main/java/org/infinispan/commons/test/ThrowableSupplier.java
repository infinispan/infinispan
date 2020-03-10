package org.infinispan.commons.test;

/**
 * Supplier that can throw any exception.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
@FunctionalInterface
public interface ThrowableSupplier<T> {
   T get() throws Throwable;
}
