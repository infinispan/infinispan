package org.infinispan.cli.impl;

import java.util.Properties;

import org.infinispan.cli.logging.Messages;
import org.infinispan.commons.util.Util;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class KubernetesContext extends ContextImpl {
   private final KubernetesClient kubernetesClient;

   public KubernetesContext(Properties defaults, KubernetesClient client) {
      super(defaults);
      this.kubernetesClient = client;
   }

   public KubernetesContext(Properties defaults) {
      this(defaults, new DefaultKubernetesClient());
   }

   public static KubernetesClient getClient(ContextAwareCommandInvocation invocation) {
      if (invocation.getContext() instanceof KubernetesContext) {
         return ((KubernetesContext)invocation.getContext()).kubernetesClient;
      } else {
         throw Messages.MSG.noKubernetes();
      }
   }

   public KubernetesClient getKubernetesClient() {
      return kubernetesClient;
   }

   @Override
   public void disconnect() {
      Util.close(kubernetesClient);
      super.disconnect();
   }

}
