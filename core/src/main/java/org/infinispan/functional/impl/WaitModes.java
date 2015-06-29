package org.infinispan.functional.impl;

import org.infinispan.commons.api.functional.Closeables;
import org.infinispan.commons.api.functional.Param;
import org.infinispan.commons.api.functional.Param.WaitMode;
import org.infinispan.commons.api.functional.Traversable;
import org.infinispan.commons.util.CloseableIterator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class WaitModes {

   static <T> CompletableFuture<T> withWaitFuture(Param<WaitMode> waitParam, ExecutorService asyncExec, Supplier<T> s) {
      switch (waitParam.get()) {
         case BLOCKING:
            // If blocking, complete the future directly with the result.
            // No separate thread or executor is instantiated.
            return CompletableFuture.completedFuture(s.get());
         case NON_BLOCKING:
            // If non-blocking execute the supply function asynchronously,
            // and return a future that's completed when the supply
            // function returns.
            return CompletableFuture.supplyAsync(s, asyncExec);
         default:
            throw new IllegalStateException();
      }
   }

   static <T> Traversable<T> withWaitTraversable(Param<WaitMode> waitParam, Supplier<Stream<T>> s) {
      switch (waitParam.get()) {
         case BLOCKING:
            return Traversables.eager(s.get());
         case NON_BLOCKING:
            return Traversables.of(s.get());
         default:
            throw new IllegalStateException("Not a valid option");
      }
   }

   static <T> CloseableIterator<T> withWaitIterator(Param<WaitMode> waitParam, Supplier<Stream<T>> s) {
      switch (waitParam.get()) {
         case BLOCKING:
            return Iterators.eager(s.get());
         case NON_BLOCKING:
            return Iterators.of(s.get());
         default:
            throw new IllegalStateException("Not yet implemented");
      }
   }

   private WaitModes() {
      // Cannot be instantiated, it's just a holder class
   }

}
