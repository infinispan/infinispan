package org.infinispan.cli.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.testing.jupiter.tags.Cli;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @since 16.3
 **/
@Cli
public class KubernetesContextTest {

   private static KubernetesClient stubClient(AtomicInteger closed) {
      return (KubernetesClient) Proxy.newProxyInstance(
            KubernetesContextTest.class.getClassLoader(),
            new Class[]{KubernetesClient.class},
            (proxy, method, args) -> {
               if ("close".equals(method.getName())) {
                  closed.incrementAndGet();
               }
               return null;
            });
   }

   @Test
   public void testClientIsCreatedOnFirstUseOnly() {
      AtomicInteger created = new AtomicInteger();
      AtomicInteger closed = new AtomicInteger();
      KubernetesContext context = new KubernetesContext(new Properties(), () -> {
         created.incrementAndGet();
         return stubClient(closed);
      });

      // Commands such as `help` and `version` must not require a Kubernetes configuration
      assertEquals(0, created.get());

      KubernetesClient client = context.getKubernetesClient();
      assertEquals(1, created.get());
      assertSame(client, context.getKubernetesClient());
      assertEquals(1, created.get());

      context.disconnect();
      assertEquals(1, closed.get());
   }

   @Test
   public void testClientCreationFailureIsDeferred() {
      // A broken or missing kubeconfig must not prevent the CLI from starting
      KubernetesContext context = new KubernetesContext(new Properties(), () -> {
         throw new IllegalStateException("no kubeconfig");
      });
      assertThrows(IllegalStateException.class, context::getKubernetesClient);
   }

   @Test
   public void testDisconnectWithoutClient() {
      AtomicInteger created = new AtomicInteger();
      KubernetesContext context = new KubernetesContext(new Properties(), () -> {
         created.incrementAndGet();
         return stubClient(new AtomicInteger());
      });
      context.disconnect();
      assertEquals(0, created.get());
   }

   @Test
   public void testClientIsRecreatedAfterDisconnect() {
      AtomicInteger created = new AtomicInteger();
      AtomicInteger closed = new AtomicInteger();
      KubernetesContext context = new KubernetesContext(new Properties(), () -> {
         created.incrementAndGet();
         return stubClient(closed);
      });

      KubernetesClient first = context.getKubernetesClient();
      context.disconnect();
      assertEquals(1, closed.get());

      KubernetesClient second = context.getKubernetesClient();
      assertEquals(2, created.get());
      assertNotSame(first, second);
   }

   @Test
   public void testSuppliedClientIsClosedOnDisconnect() {
      // A client supplied by the caller is owned by the context, even if no command ever used it
      AtomicInteger closed = new AtomicInteger();
      KubernetesClient client = stubClient(closed);
      KubernetesContext context = new KubernetesContext(new Properties(), client);

      context.disconnect();
      assertEquals(1, closed.get());

      // The supplied client cannot be recreated, so the context must not hand out the closed one again
      assertThrows(IllegalStateException.class, context::getKubernetesClient);
   }

   @Test
   public void testConcurrentCreationYieldsASingleClient() throws Exception {
      int threads = 8;
      AtomicInteger created = new AtomicInteger();
      AtomicInteger closed = new AtomicInteger();
      CyclicBarrier barrier = new CyclicBarrier(threads);
      KubernetesContext context = new KubernetesContext(new Properties(), () -> {
         created.incrementAndGet();
         return stubClient(closed);
      });

      ExecutorService executor = Executors.newFixedThreadPool(threads);
      try {
         List<Callable<KubernetesClient>> tasks = new ArrayList<>(threads);
         for (int i = 0; i < threads; i++) {
            tasks.add(() -> {
               barrier.await();
               return context.getKubernetesClient();
            });
         }
         KubernetesClient first = null;
         for (Future<KubernetesClient> future : executor.invokeAll(tasks)) {
            KubernetesClient client = future.get();
            if (first == null) {
               first = client;
            }
            assertSame(first, client);
         }
      } finally {
         executor.shutdownNow();
      }

      assertEquals(1, created.get());
      context.disconnect();
      assertEquals(1, closed.get());
   }
}
