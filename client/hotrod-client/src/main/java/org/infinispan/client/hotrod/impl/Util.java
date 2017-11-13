package org.infinispan.client.hotrod.impl;

import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.commons.CacheException;

public class Util {
   private Util() {
   }

   public static <T> T await(CompletableFuture<T> cf) {
      try {
         return cf.get();
      } catch (InterruptedException e) {
         throw new CacheException(e);
      } catch (ExecutionException e) {
         throw rewrap(e);
      }
   }

   public static <T> T await(CompletableFuture<T> cf, long timeoutMillis) {
      try {
         return cf.get(timeoutMillis, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
         throw new CacheException(e);
      } catch (ExecutionException e) {
         throw rewrap(e);
      } catch (TimeoutException e) {
         cf.cancel(false);
         throw new TransportException(new SocketTimeoutException(), null);
      }
   }

   protected static RuntimeException rewrap(ExecutionException e) {
      if (e.getCause() instanceof HotRodClientException) {
         return (HotRodClientException) e.getCause();
      } else if (e.getCause() instanceof CacheException) {
         return (CacheException) e.getCause();
      } else {
         return new TransportException(e.getCause(), null);
      }
   }
}
