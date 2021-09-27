package org.infinispan.cli.commands.kubernetes;

import static org.infinispan.cli.commands.kubernetes.Kube.DEFAULT_CLUSTER_NAME;
import static org.infinispan.cli.commands.kubernetes.Kube.INFINISPAN_CLUSTER_CRD;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.KubernetesContext;
import org.kohsuke.MetaInfServices;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@GroupCommandDefinition(
      name = "delete",
      description = "Deletes resources.",
      groupCommands = {
            Delete.Cluster.class,
      })
public class Delete extends CliCommand {

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      return CommandResult.FAILURE;
   }

   @MetaInfServices(Command.class)
   @CommandDefinition(name = "cluster", description = "Deletes a cluster")
   public static class Cluster extends CliCommand {

      @Option(shortName = 'n', description = "Specifies the namespace of the cluster to delete. Uses the default namespace if you do not specify one.")
      String namespace;

      @Argument(description = "Specifies the name of the cluster to delete. Defaults to '" + DEFAULT_CLUSTER_NAME + "'", defaultValue = DEFAULT_CLUSTER_NAME)
      String name;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         KubernetesClient client = KubernetesContext.getClient(invocation);
         client.genericKubernetesResources(INFINISPAN_CLUSTER_CRD).inNamespace(Kube.getNamespaceOrDefault(client, namespace)).withName(name).delete();
         return CommandResult.SUCCESS;
      }
   }
}
