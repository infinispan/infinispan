package org.infinispan.commons.util.concurrent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Utility methods for {@link CompletableFuture}.
 *
 * @since 14.0
 */
public final class CompletableFutures {

   private static final CompletableFuture<Boolean> completedTrueFuture = CompletableFuture.completedFuture(Boolean.TRUE);
   private static final CompletableFuture<Boolean> completedFalseFuture = CompletableFuture.completedFuture(Boolean.FALSE);
   private static final CompletableFuture<?> completedNullFuture = CompletableFuture.completedFuture(null);
   private static final Function<?, ?> identity = t -> t;

   private CompletableFutures() {
   }

   public static CompletionException asCompletionException(Throwable t) {
      if (t instanceof CompletionException) {
         return ((CompletionException) t);
      } else {
         return new CompletionException(t);
      }
   }

   public static void rethrowExceptionIfPresent(Throwable t) {
      if (t != null) {
         throw asCompletionException(t);
      }
   }

   public static Throwable extractException(Throwable t) {
      Throwable cause = t.getCause();
      if (cause != null && t instanceof CompletionException) {
         return cause;
      }
      return t;
   }

   @SuppressWarnings("unchecked")
   public static <T> CompletableFuture<T> completedNull() {
      return (CompletableFuture<T>) completedNullFuture;
   }

   public static CompletableFuture<Boolean> completedTrue() {
      return completedTrueFuture;
   }

   public static CompletableFuture<Boolean> completedFalse() {
      return completedFalseFuture;
   }

   public static CompletionStage<Boolean> booleanStage(boolean trueOrFalse) {
      return trueOrFalse ? completedTrueFuture : completedFalseFuture;
   }

   @SuppressWarnings("unchecked")
   public static <T> Function<T, T> identity() {
      return (Function<T, T>) identity;
   }
}
