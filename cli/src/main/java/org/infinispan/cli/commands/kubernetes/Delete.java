package org.infinispan.cli.commands.kubernetes;

import static org.infinispan.cli.commands.kubernetes.Kube.DEFAULT_CLUSTER_NAME;
import static org.infinispan.cli.commands.kubernetes.Kube.INFINISPAN_CLUSTER_CRD;

import java.io.IOException;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.KubernetesContextImpl;
import org.kohsuke.MetaInfServices;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(
      name = Delete.CMD,
      description = "Deletes a resource",
      groupCommands = {
            Delete.Cluster.class,
      })
public class Delete extends CliCommand {
   public static final String CMD = "delete";

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
   @CommandDefinition(name = Cluster.CMD, description = "Deletes a cluster")
   public static class Cluster extends CliCommand {
      public static final String CMD = "cluster";

      @Option(shortName = 'n', description = "Select the namespace")
      String namespace;

      @Argument(description = "The name of the service to delete", defaultValue = DEFAULT_CLUSTER_NAME)
      String name;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         KubernetesClient client = ((KubernetesContextImpl)invocation.getContext()).getKubernetesClient();
         try {
            client.customResource(INFINISPAN_CLUSTER_CRD).delete(Kube.getNamespaceOrDefault(client, namespace), name);
            return CommandResult.SUCCESS;
         } catch (IOException e) {
            return CommandResult.FAILURE;
         }
      }
   }
}
