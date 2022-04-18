package org.infinispan.counter.impl;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.infinispan.counter.exception.CounterException;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * Utility methods.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public final class Util {

   private Util() {
   }

   /**
    * Returns a {@link CounterException} with the throwable.
    */
   private static CounterException rethrowAsCounterException(Throwable throwable) {
      //make it public if needed.
      if (throwable instanceof CounterException) {
         return (CounterException) throwable;
      } else if (throwable instanceof ExecutionException || throwable instanceof CompletionException) {
         return rethrowAsCounterException(throwable.getCause());
      } else {
         return new CounterException(throwable);
      }
   }

   /**
    * Awaits for the counter operation and throws any exception as {@link CounterException}.
    */
   public static <T> T awaitCounterOperation(CompletionStage<T> stage) {
      try {
         return CompletionStages.await(stage);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw rethrowAsCounterException(e);
      } catch (ExecutionException e) {
         if (e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
         }
         throw rethrowAsCounterException(e.getCause());
      }
   }
}
