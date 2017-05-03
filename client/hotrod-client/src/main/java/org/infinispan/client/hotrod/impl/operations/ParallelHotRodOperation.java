package org.infinispan.client.hotrod.impl.operations;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.ParallelOperationException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * An HotRod operation that span across multiple remote nodes concurrently (like getAll / putAll).
 *
 * @author Guillaume Darmont / guillaume@dropinocean.com
 */
public abstract class ParallelHotRodOperation<T, SUBOP extends HotRodOperation> extends HotRodOperation {

   private static final Log log = LogFactory.getLog(ParallelHotRodOperation.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   protected final TransportFactory transportFactory;
   protected final CompletionService<T> completionService;

   protected ParallelHotRodOperation(Codec codec, TransportFactory transportFactory, byte[] cacheName, AtomicInteger
         topologyId, int flags, Configuration cfg, ExecutorService executorService) {
      super(codec, flags, cfg, cacheName, topologyId);
      this.transportFactory = transportFactory;
      this.completionService = new ExecutorCompletionService<>(executorService);
   }

   @Override
   public T execute() {
      List<SUBOP> operations = mapOperations();

      if (operations.isEmpty()) {
         return createCollector();
      } else if (operations.size() == 1) {
         // Only one operation to do, we stay in the caller thread
         return executeSequential(operations.get(0));
      } else {
         // Multiple operation, submit to the thread poll
         return executeParallel(operations);
      }
   }

   private T executeSequential(SUBOP subop) {
      T collector = createCollector();
      combine(collector, (T) subop.execute());
      return collector;
   }

   private T executeParallel(List<SUBOP> operations) {
      Set<Future<T>> remainingTasks = new HashSet<>(operations.size());
      for (SUBOP operation : operations) {
         remainingTasks.add(completionService.submit(() -> (T) operation.execute()));
      }

      T collector = createCollector();

      for (int i = 0; i < operations.size(); i++) {
         try {
            Future<T> result = completionService.take();
            combine(collector, result.get());
            remainingTasks.remove(result);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cancelRemainingTasks(remainingTasks);
            throw new ParallelOperationException(e);
         } catch (ExecutionException | RuntimeException e) {
            cancelRemainingTasks(remainingTasks);
            throw new ParallelOperationException(e);
         }
      }
      return collector;
   }

   private void cancelRemainingTasks(Set<Future<T>> remainingTasks) {
      remainingTasks.forEach(task -> task.cancel(true));
   }

   protected abstract List<SUBOP> mapOperations();

   protected abstract T createCollector();

   protected abstract void combine(T collector, T result);
}
