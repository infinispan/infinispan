package org.infinispan.cli.commands.kubernetes;

import java.util.Map;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.KubernetesContext;
import org.infinispan.cli.logging.Messages;
import org.infinispan.commons.util.Version;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@CommandDefinition(name = "uninstall", description = "Removes the Operator.")
public class Uninstall extends CliCommand {

   @Option(shortName = 'n', description = "Specifies the namespace where the Operator is installed, if you did not install it globally.")
   String namespace;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      KubernetesClient client = KubernetesContext.getClient(invocation);
      if (namespace == null) {
         namespace = Kube.defaultOperatorNamespace(client);
      } else {
         // We need to remove the operator group
         client.genericKubernetesResources(Kube.OPERATOR_OPERATORGROUP_CRD).inNamespace(namespace).withName(Version.getProperty("infinispan.olm.name")).delete();
      }
      // Obtain the CSV for the subscription
      Resource<GenericKubernetesResource> subscription = client.genericKubernetesResources(Kube.OPERATOR_SUBSCRIPTION_CRD).inNamespace(namespace).withName(Version.getProperty("infinispan.olm.name"));
      GenericKubernetesResource sub = subscription.get();
      if (sub == null) {
         throw Messages.MSG.noOperatorSubscription(namespace);
      }
      Map<String, Object> status = (Map<String, Object>) sub.getAdditionalProperties().get("status");
      String csv = (String) status.get("installedCSV");
      boolean deleted = subscription.delete().size() > 0;
      if (deleted) {
         // Now delete the CSV
         deleted = client.genericKubernetesResources(Kube.OPERATOR_CLUSTERSERVICEVERSION_CRD).inNamespace(namespace).withName(csv).delete().size() > 0;
         return deleted ? CommandResult.SUCCESS : CommandResult.FAILURE;
      } else {
         return CommandResult.FAILURE;
      }
   }
}
