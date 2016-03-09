package org.infinispan.stream.impl.local;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.intops.UnorderedOperation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.Set;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * Implements the base operations required for a local stream.
 * stream is populated
 */
public abstract class AbstractLocalCacheStream<T, S extends BaseStream<T, S>, S2 extends S> implements BaseStream<T, S> {
   protected final Log log = LogFactory.getLog(getClass());

   protected final StreamSupplier<T> streamSupplier;
   protected final ComponentRegistry registry;

   protected final Collection<Runnable> onCloseRunnables;
   protected final Queue<IntermediateOperation> intermediateOperations;

   protected Set<Integer> segmentsToFilter;
   protected Set<?> keysToFilter;
   protected boolean parallel;

   public interface StreamSupplier<R> {
      Stream<R> buildStream(Set<Integer> segmentsToFilter, Set<?> keysToFilter);

      CloseableIterator<R> removableIterator(CloseableIterator<R> realIterator);
   }

   /**
    * @param streamSupplier
    * @param parallel
    * @param registry
    */
   public AbstractLocalCacheStream(StreamSupplier<T> streamSupplier, boolean parallel, ComponentRegistry registry) {
      this.streamSupplier = streamSupplier;
      this.registry = registry;

      this.onCloseRunnables = new ArrayList<>(4);
      this.intermediateOperations = new ArrayDeque<>();

      this.parallel = parallel;
   }

   AbstractLocalCacheStream(AbstractLocalCacheStream<?, ?, ?> original) {
      this.streamSupplier = (StreamSupplier<T>) original.streamSupplier;
      this.registry = original.registry;

      this.onCloseRunnables = original.onCloseRunnables;
      this.intermediateOperations = original.intermediateOperations;

      this.segmentsToFilter = original.segmentsToFilter;
      this.keysToFilter = original.keysToFilter;

      this.parallel = original.parallel;
   }

   protected final S createStream() {
      BaseStream<?, ?> stream = streamSupplier.buildStream(segmentsToFilter, keysToFilter);
      if (parallel) {
         stream = stream.parallel();
      }
      for (IntermediateOperation intOp : intermediateOperations) {
         stream = intOp.perform(stream);
      }
      return (S) stream;
   }

   @Override
   public boolean isParallel() {
      return createStream().isParallel();
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
