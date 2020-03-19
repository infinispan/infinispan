package org.infinispan.cli.impl;

import java.util.Properties;

import org.infinispan.commons.util.Util;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class KubernetesContextImpl extends ContextImpl {
   private final KubernetesClient kubernetesClient;

   public KubernetesContextImpl(Properties defaults, KubernetesClient client) {
      super(defaults);
      this.kubernetesClient = client;
   }

   public KubernetesContextImpl(Properties defaults) {
      this(defaults, new DefaultKubernetesClient());
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
