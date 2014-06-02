package org.infinispan.commons.util.concurrent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Converts the result values for wrapped future using the provided converter.
 */
public class ConvertingNotifyingFuture<TSource, TResult> implements NotifyingFuture<TResult> {
   private NotifyingFuture<TSource> future;
   private Converter<TSource, TResult> converter;

   public ConvertingNotifyingFuture(NotifyingFuture<TSource> future, Converter<TSource, TResult> converter) {
      this.future = future;
      this.converter = converter;
   }

   public interface Converter<TSource, TResult> {
      TResult convert(TSource source);
   }

   /**
    * Converts any result to Void, returning null.
    * @param <TSource>
    */
   public static class VoidConverter<TSource> implements Converter<TSource, Void> {
      @Override
      public Void convert(TSource tSource) {
         return null;
      }
   }

   @Override
   public NotifyingFuture<TResult> attachListener(final FutureListener<TResult> listener) {
      future.attachListener(new FutureListener<TSource>() {
         @Override
         public void futureDone(Future<TSource> future) {
            listener.futureDone(ConvertingNotifyingFuture.this);
         }
      });
      return this;
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      return future.cancel(mayInterruptIfRunning);
   }

   @Override
   public boolean isCancelled() {
      return future.isCancelled();
   }

   @Override
   public boolean isDone() {
      return future.isDone();
   }

   @Override
   public TResult get() throws InterruptedException, ExecutionException {
      return converter.convert(future.get());
   }

   @Override
   public TResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return converter.convert(future.get(timeout, unit));
   }
}
