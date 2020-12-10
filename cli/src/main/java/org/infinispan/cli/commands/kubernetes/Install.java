package org.infinispan.cli.commands.kubernetes;

import static org.infinispan.cli.commands.kubernetes.Kube.OPERATOR_OPERATORGROUP_CRD;
import static org.infinispan.cli.commands.kubernetes.Kube.OPERATOR_SUBSCRIPTION_CRD;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.KubernetesContextImpl;
import org.infinispan.commons.util.TypedProperties;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@CommandDefinition(name = "install", description = "Installs the operator")
public class Install extends CliCommand {

   @Option(shortName = 'n', description = "Select the namespace")
   String namespace;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      KubernetesClient client = ((KubernetesContextImpl) invocation.getContext()).getKubernetesClient();
      namespace = Kube.getNamespaceOrDefault(client, namespace);

      try {
         TypedProperties properties = new TypedProperties().setProperty("NAMESPACE", this.namespace);
         String crd = Kube.loadResourceAsString("/operator/operator-install.yaml", properties);
         client.load(new ByteArrayInputStream(crd.getBytes(StandardCharsets.UTF_8))).inNamespace(this.namespace).createOrReplace();
         crd = Kube.loadResourceAsString("/operator/operator-group.yaml", properties);
         client.customResource(OPERATOR_OPERATORGROUP_CRD).createOrReplace(this.namespace, crd);
         crd = Kube.loadResourceAsString("/operator/operator-subscription.yaml", properties);
         client.customResource(OPERATOR_SUBSCRIPTION_CRD).createOrReplace(this.namespace, crd);
         return CommandResult.SUCCESS;
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }
}
