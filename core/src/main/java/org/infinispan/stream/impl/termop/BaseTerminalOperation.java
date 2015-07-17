package org.infinispan.stream.impl.termop;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.impl.KeyTrackingTerminalOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.io.Serializable;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * Abstract instance that provides common code needed for all terminal operations.  Note this class doesn't extend
 * any interface due to the fact that different terminal operations have possibly different interfaces to implement
 * however all of them require the information stored here.
 */
public abstract class BaseTerminalOperation {
   protected final Iterable<IntermediateOperation> intermediateOperations;
   protected transient Supplier<? extends BaseStream<?, ?>> supplier;

   protected BaseTerminalOperation(Iterable<IntermediateOperation> intermediateOperations,
                                   Supplier<? extends BaseStream<?, ?>> supplier) {
      this.intermediateOperations = intermediateOperations;
      this.supplier = supplier;
   }

   public Iterable<IntermediateOperation> getIntermediateOperations() {
      return intermediateOperations;
   }

   public void setSupplier(Supplier<? extends Stream<?>> supplier) {
      this.supplier = supplier;
   }

   public void handleInjection(ComponentRegistry registry) {
      intermediateOperations.forEach(i -> i.handleInjection(registry));
   }
}
