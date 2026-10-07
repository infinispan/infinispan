package org.infinispan.cli.impl;

import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;

import org.infinispan.cli.logging.Messages;
import org.infinispan.commons.util.Util;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class KubernetesContext extends ContextImpl {
   /**
    * Creates a client on demand. {@code null} when the context was built around a client supplied by the caller,
    * which cannot be recreated once it has been closed.
    */
   private final Supplier<KubernetesClient> clientSupplier;
   private KubernetesClient kubernetesClient;

   /**
    * Creates a context around an already built client. The context takes ownership of the client and closes it on
    * {@link #disconnect()}; since such a client cannot be recreated, the context is unusable afterwards.
    */
   public KubernetesContext(Properties defaults, KubernetesClient client) {
      super(defaults);
      this.clientSupplier = null;
      this.kubernetesClient = Objects.requireNonNull(client);
   }

   public KubernetesContext(Properties defaults) {
      this(defaults, () -> new KubernetesClientBuilder().build());
   }

   KubernetesContext(Properties defaults, Supplier<KubernetesClient> clientSupplier) {
      super(defaults);
      this.clientSupplier = Objects.requireNonNull(clientSupplier);
   }

   public static KubernetesClient getClient(ContextAwareCommandInvocation invocation) {
      if (invocation.getContext() instanceof KubernetesContext) {
         return ((KubernetesContext) invocation.getContext()).getKubernetesClient();
      } else {
         throw Messages.MSG.noKubernetes();
      }
   }

   /**
    * Returns the {@link KubernetesClient}, creating it on first use. Creating a client requires a usable
    * configuration (e.g. a kubeconfig file), so it is deferred until a command actually needs to talk to the
    * cluster: commands such as <code>help</code> and <code>version</code> must work without a cluster.
    */
   public synchronized KubernetesClient getKubernetesClient() {
      if (kubernetesClient == null) {
         if (clientSupplier == null) {
            throw Messages.MSG.kubernetesClientClosed();
         }
         kubernetesClient = clientSupplier.get();
      }
      return kubernetesClient;
   }

   @Override
   public synchronized void disconnect() {
      Util.close(kubernetesClient);
      kubernetesClient = null;
      super.disconnect();
   }

}
