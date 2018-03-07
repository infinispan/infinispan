package org.infinispan.stream.impl.termop;

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.impl.intops.IntermediateOperation;

/**
 * Abstract instance that provides common code needed for all terminal operations.  Note this class doesn't extend
 * any interface due to the fact that different terminal operations have possibly different interfaces to implement
 * however all of them require the information stored here.
 * @param <Original> original stream type
 */
public abstract class BaseTerminalOperation<Original> {
   protected final Iterable<IntermediateOperation> intermediateOperations;
   protected transient Supplier<Stream<Original>> supplier;

   protected BaseTerminalOperation(Iterable<IntermediateOperation> intermediateOperations,
                                   Supplier<Stream<Original>> supplier) {
      this.intermediateOperations = intermediateOperations;
      this.supplier = supplier;
   }

   public Iterable<IntermediateOperation> getIntermediateOperations() {
      return intermediateOperations;
   }

   public void setSupplier(Supplier<Stream<Original>> supplier) {
      this.supplier = supplier;
   }

   public void handleInjection(ComponentRegistry registry) {
      intermediateOperations.forEach(i -> i.handleInjection(registry));
   }
}
