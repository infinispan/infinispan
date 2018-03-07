package org.infinispan.stream.impl;

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.infinispan.factories.ComponentRegistry;

/**
 * Interface describing an operation that is a terminal one that doesn't track keys.
 * @param <R> the returing result
 */
public interface TerminalOperation<Original, R> extends SegmentAwareOperation {
   /**
    * Actually runs the terminal operation returning the result from the operation
    * @return the value retrieved for the operation
    */
   R performOperation();

   /**
    * Sets the local supplier for the stream.  This is to be invoked on a remote node after the object is unmarshalled
    * to set the supplier to use
    * @param supplier the supplier that will return the stream that the operations are performed on
    */
   void setSupplier(Supplier<Stream<Original>> supplier);

   /**
    * Handles injection of components for various intermediate and this operation.
    * @param registry component registry to use
    */
   void handleInjection(ComponentRegistry registry);
}
