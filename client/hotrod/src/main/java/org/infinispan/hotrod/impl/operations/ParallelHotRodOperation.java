package org.infinispan.hotrod.impl.operations;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

/**
 * An HotRod operation that span across multiple remote nodes concurrently (like getAll / putAll).
 */
public abstract class ParallelHotRodOperation<T, SUBOP extends HotRodOperation<T>> extends StatsAffectingHotRodOperation<T> {
   protected ParallelHotRodOperation(OperationContext operationContext, CacheOptions options, DataFormat dataFormat) {
      super(operationContext, ILLEGAL_OP_CODE, ILLEGAL_OP_CODE, options, dataFormat);
   }

   @Override
   public CompletableFuture<T> execute() {
      List<SUBOP> operations = mapOperations();

      if (operations.isEmpty()) {
         return CompletableFuture.completedFuture(createCollector());
      } else if (operations.size() == 1) {
         // Only one operation to do, we stay in the caller thread
         return operations.get(0).execute().toCompletableFuture();
      } else {
         // Multiple operation, submit to the thread poll
         return executeParallel(operations);
      }
   }

   private CompletableFuture<T> executeParallel(List<SUBOP> operations) {
      T collector = createCollector();
      AtomicInteger counter = new AtomicInteger(operations.size());
      for (SUBOP operation : operations) {
         operation.execute().whenComplete((result, throwable) -> {
            if (throwable != null) {
               completeExceptionally(throwable);
            } else {
               if (collector != null) {
                  synchronized (collector) {
                     combine(collector, result);
                  }
               }
               if (counter.decrementAndGet() == 0) {
                  complete(collector);
               }
            }
         });
      }
      this.exceptionally(throwable -> {
         for (SUBOP operation : operations) {
            operation.cancel(true);
         }
         return null;
      });
      return this;
   }

   protected abstract List<SUBOP> mapOperations();

   protected abstract T createCollector();

   protected abstract void combine(T collector, T result);

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      throw new UnsupportedOperationException();
   }
}
