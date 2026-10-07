package org.infinispan.cli.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import java.util.Properties;
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
}
