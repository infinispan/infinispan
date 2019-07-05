package org.infinispan.test;

/**
 * Supplier that can throw any exception.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
@FunctionalInterface
public interface ExceptionSupplier<T> {
   T get() throws Exception;
}
