package org.infinispan.client.hotrod.impl;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.operations.ManagerOperationsFactory;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.WrappedByteArray;

import io.netty.handler.codec.DecoderException;

public class Util {
   private static final long BIG_DELAY_NANOS = TimeUnit.DAYS.toNanos(1);
   private static final Xid DUMMY_XID = new Xid() {
      @Override
      public int getFormatId() {
         return 0;
      }

      @Override
      public byte[] getGlobalTransactionId() {
         return new byte[]{1};
      }

      @Override
      public byte[] getBranchQualifier() {
         return new byte[]{1};
      }
   };

   private Util() {
   }

   public static <T> T await(CompletionStage<T> cf) {
      return await(cf.toCompletableFuture());
   }

   public static <T> T await(CompletableFuture<T> cf) {
      try {
         // timed wait does not do busy waiting
         return cf.get(BIG_DELAY_NANOS, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
         // Need to restore interrupt status because InterruptedException cannot be sent back as is
         Thread.currentThread().interrupt();
         throw new HotRodClientException(e);
      } catch (ExecutionException e) {
         throw rewrap(e);
      } catch (TimeoutException e) {
         throw new IllegalStateException(e);
      }
   }

   public static <T> T await(CompletionStage<T> cf, long timeoutMillis) {
      return await(cf.toCompletableFuture(), timeoutMillis);
   }

   public static <T> T await(CompletableFuture<T> cf, long timeoutMillis) {
      try {
         return cf.get(timeoutMillis, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
         // Need to restore interrupt status because InterruptedException cannot be sent back as is
         Thread.currentThread().interrupt();
         throw new HotRodClientException(e);
      } catch (ExecutionException e) {
         throw rewrap(e);
      } catch (TimeoutException e) {
         cf.cancel(false);
         throw new TransportException(new SocketTimeoutException(), null);
      }
   }

   public static HotRodClientException rewrap(Throwable t) {
      Throwable cause = t;
      while ((cause instanceof CompletionException || cause instanceof DecoderException ||
            cause instanceof TransportException) && cause.getCause() != null) {
         cause = cause.getCause();
      }
      if (cause instanceof HotRodClientException hrce) {
         return hrce;
      } else if (t instanceof HotRodClientException hrce) {
         return hrce;
      } else {
         return new TransportException(t, null);
      }
   }

   protected static RuntimeException rewrap(ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof HotRodClientException) {
         cause.setStackTrace(e.getStackTrace());
         return (HotRodClientException) cause;
      } else if (cause instanceof CacheException) {
         return new CacheException(cause);
      } else {
         return new TransportException(cause, null);
      }
   }

   public static CompletionStage<Boolean> checkTransactionSupport(String cacheName, ManagerOperationsFactory operationsFactory, OperationDispatcher dispatcher) {
      HotRodOperation<Integer> op = operationsFactory.newPrepareTransaction(cacheName, DUMMY_XID, true, Collections.emptyList(),
            false, 60000);
      return dispatcher.execute(op).handle((integer, throwable) -> {
         if (throwable != null) {
            HOTROD.invalidTxServerConfig(cacheName, throwable);
         }
         return throwable == null;
      });
   }

   public static boolean checkTransactionSupport(String cacheName, ManagerOperationsFactory factory, OperationDispatcher dispatcher, Log log) {
      HotRodOperation<Integer> op = factory.newPrepareTransaction(cacheName, DUMMY_XID, true, Collections.emptyList(),
            false, 60000);
      try {
         return dispatcher.execute(op).handle((integer, throwable) -> {
            if (throwable != null) {
               HOTROD.invalidTxServerConfig(cacheName, throwable);
            }
            return throwable == null;
         }).toCompletableFuture().get();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
         log.debugf("Exception while checking transaction support in server", e);
      }
      return false;
   }

   public static WrappedByteArray wrapBytes(byte[] cacheName) {
      WrappedByteArray wrappedCacheName;
      if (cacheName == null || cacheName.length == 0) {
         wrappedCacheName = WrappedByteArray.EMPTY_BYTES;
      } else {
         wrappedCacheName = new WrappedByteArray(cacheName);
      }
      return wrappedCacheName;
   }
}
