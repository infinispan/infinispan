package org.infinispan.testing;

@FunctionalInterface
public interface ThrowingSupplier<T> {
   T get() throws Throwable;
}
