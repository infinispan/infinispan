package org.infinispan.cli.commands.kubernetes;

import static org.infinispan.cli.commands.kubernetes.Kube.DEFAULT_CLUSTER_NAME;
import static org.infinispan.cli.commands.kubernetes.Kube.INFINISPAN_CLUSTER_CRD;
import static org.infinispan.cli.commands.kubernetes.Kube.add;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.ExposeCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.KubernetesContextImpl;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/

@GroupCommandDefinition(
      name = "create",
      description = "Creates a resource",
      groupCommands = {
            Create.Cluster.class,
      })
public class Create extends CliCommand {

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

   @CommandDefinition(name = "cluster", description = "Creates a cluster")
   public static class Cluster extends CliCommand {

      @Option(shortName = 'n', description = "Select the namespace")
      String namespace;

      @Option(shortName = 'r', description = "The number of replicas", defaultValue = "1")
      int replicas;

      @Option(name = "expose-type", completer = ExposeCompleter.class)
      String exposeType;

      @Option(name = "expose-port", defaultValue = "0")
      int exposePort;

      @Option(name = "expose-host")
      String exposeHost;

      @Argument(description = "The name of the cluster to create", defaultValue = DEFAULT_CLUSTER_NAME)
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

         Map<String, Object> resource = new HashMap<>();
         add(resource, "apiVersion", "infinispan.org/v1");
         add(resource, "kind", "Infinispan");
         add(resource, "metadata.name", name);
         add(resource, "spec.replicas", replicas);
         if (exposeType != null) {
            add(resource, "spec.expose.type", exposeType);
         }
         if (exposeHost != null) {
            add(resource, "spec.expose.host", exposeHost);
         }
         if (exposePort > 0) {
            add(resource, "spec.expose.port", exposePort);
         }

         try {
            client.customResource(INFINISPAN_CLUSTER_CRD).create(Kube.getNamespaceOrDefault(client, namespace), resource);
            return CommandResult.SUCCESS;
         } catch (IOException e) {
            return CommandResult.SUCCESS;
         }
      }
   }
}
