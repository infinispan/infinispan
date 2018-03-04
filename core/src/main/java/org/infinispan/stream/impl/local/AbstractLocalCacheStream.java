package org.infinispan.stream.impl.local;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.Set;
import java.util.stream.BaseStream;

import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.intops.UnorderedOperation;

/**
 * Implements the base operations required for a local stream.
 * stream is populated
 */
public abstract class AbstractLocalCacheStream<T, S extends BaseStream<T, S>, S2 extends S> implements BaseStream<T, S> {
   protected final StreamSupplier<T, S> streamSupplier;
   protected final ComponentRegistry registry;

   protected final Collection<Runnable> onCloseRunnables;
   protected final Queue<IntermediateOperation> intermediateOperations;

   protected IntSet segmentsToFilter;
   protected Set<?> keysToFilter;
   protected boolean parallel;

   public interface StreamSupplier<T, S extends BaseStream<T, S>> {
      S buildStream(IntSet segmentsToFilter, Set<?> keysToFilter, boolean parallel);
   }

   /**
    * @param streamSupplier
    * @param parallel
    * @param registry
    */
   public AbstractLocalCacheStream(StreamSupplier<T, S> streamSupplier, boolean parallel, ComponentRegistry registry) {
      this.streamSupplier = streamSupplier;
      this.registry = registry;

      this.onCloseRunnables = new ArrayList<>(4);
      this.intermediateOperations = new ArrayDeque<>();

      this.parallel = parallel;
   }

   AbstractLocalCacheStream(AbstractLocalCacheStream<?, ?, ?> original) {
      this.streamSupplier = (StreamSupplier<T, S>) original.streamSupplier;
      this.registry = original.registry;

      this.onCloseRunnables = original.onCloseRunnables;
      this.intermediateOperations = original.intermediateOperations;

      this.segmentsToFilter = original.segmentsToFilter;
      this.keysToFilter = original.keysToFilter;

      this.parallel = original.parallel;
   }

   protected final S createStream() {
      BaseStream<?, ?> stream = streamSupplier.buildStream(segmentsToFilter, keysToFilter, parallel);
      for (IntermediateOperation intOp : intermediateOperations) {
         intOp.handleInjection(registry);
         stream = intOp.perform(stream);
      }
      return (S) stream;
   }

   @Override
   public boolean isParallel() {
      return parallel;
   }

   @Override
   public S2 sequential() {
      this.parallel = false;
      return (S2) this;
   }

   @Override
   public S2 parallel() {
      this.parallel = true;
      return (S2) this;
   }

   @Override
   public S2 unordered() {
      intermediateOperations.add(new UnorderedOperation<>());
      return (S2) this;
   }

   @Override
   public S2 onClose(Runnable closeHandler) {
      onCloseRunnables.add(closeHandler);
      return (S2) this;
   }

   @Override
   public void close() {
      onCloseRunnables.forEach(Runnable::run);
   }
}
