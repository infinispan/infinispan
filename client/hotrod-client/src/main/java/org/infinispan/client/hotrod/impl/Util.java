package org.infinispan.client.hotrod.impl;

import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.transaction.operations.PrepareTransactionOperation;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.commons.CacheException;

public class Util {
   private final static long BIG_DELAY_NANOS = TimeUnit.DAYS.toNanos(1);
   private static final Xid DUMMY_XID = new Xid() {
      @Override
      public int getFormatId() {
         return 0;
      }

      @Override
      public byte[] getGlobalTransactionId() {
         return new byte[] {1};
      }

      @Override
      public byte[] getBranchQualifier() {
         return new byte[] {1};
      }
   };

   private Util() {
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

   protected static RuntimeException rewrap(ExecutionException e) {
      if (e.getCause() instanceof HotRodClientException) {
         return (HotRodClientException) e.getCause();
      } else if (e.getCause() instanceof CacheException) {
         return (CacheException) e.getCause();
      } else {
         return new TransportException(e.getCause(), null);
      }
   }

   public static boolean checkTransactionSupport(String cacheName, OperationsFactory factory, Log log) {
      PrepareTransactionOperation op = factory.newPrepareTransactionOperation(DUMMY_XID, true, Collections.emptyList(),
            false, 60000);
      try {
         return op.execute().handle((integer, throwable) -> {
            if (throwable != null) {
               log.invalidTxServerConfig(cacheName, throwable);
            }
            return throwable == null;
         }).get();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
         log.debugf("Exception while checking transaction support in server", e);
      }
      return false;
   }
}
