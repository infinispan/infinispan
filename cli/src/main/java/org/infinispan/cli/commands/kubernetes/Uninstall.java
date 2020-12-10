package org.infinispan.cli.commands.kubernetes;

import static org.infinispan.cli.commands.kubernetes.Kube.OPERATOR_SUBSCRIPTION_CRD;

import java.io.IOException;
import java.util.Map;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.KubernetesContextImpl;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@CommandDefinition(name = "uninstall", description = "Uninstalls the operator")
public class Uninstall extends CliCommand {

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
      KubernetesClient client = ((KubernetesContextImpl)invocation.getContext()).getKubernetesClient();
      namespace = Kube.getNamespaceOrDefault(client, namespace);

      try {
         Map<String, Object> deleted = client.customResource(OPERATOR_SUBSCRIPTION_CRD).delete(namespace, "infinispan");
         return deleted != null && deleted.isEmpty() ? CommandResult.SUCCESS : CommandResult.FAILURE;
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }
}
