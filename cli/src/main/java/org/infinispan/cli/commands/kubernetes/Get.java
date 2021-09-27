package org.infinispan.cli.commands.kubernetes;

import static org.infinispan.cli.commands.kubernetes.Kube.INFINISPAN_CLUSTER_CRD;

import java.io.PrintStream;
import java.util.List;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.KubernetesContext;
import org.kohsuke.MetaInfServices;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@GroupCommandDefinition(
      name = "get",
      description = "Displays resources.",
      groupCommands = {
            Get.Clusters.class,
      })
public class Get extends CliCommand {

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
   @CommandDefinition(name = "clusters", description = "Get clusters")
   public static class Clusters extends CliCommand {

      @Option(shortName = 'n', description = "Specifies the namespace where the cluster is running. Uses the default namespace if you do not specify one.")
      String namespace;

      @Option(name = "all-namespaces", shortName = 'A', description = "Displays the requested object(s) across all namespaces.")
      boolean allNamespaces;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Option(shortName = 's', hasValue = false, description = "Displays all secrets that the cluster uses.")
      protected boolean secrets;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         KubernetesClient client = KubernetesContext.getClient(invocation);
         GenericKubernetesResourceList resource = allNamespaces ?
               client.genericKubernetesResources(INFINISPAN_CLUSTER_CRD).inAnyNamespace().list() :
               client.genericKubernetesResources(INFINISPAN_CLUSTER_CRD).inNamespace(Kube.getNamespaceOrDefault(client, namespace)).list();
         List<GenericKubernetesResource> items = resource.getItems();
         PrintStream out = invocation.getShellOutput();
         out.printf("%-32s %-16s %-9s %-16s%n", "NAME", "NAMESPACE", "STATUS", "SECRETS");
         items.forEach(item -> {
            String n = item.getMetadata().getName();
            String ns = item.getMetadata().getNamespace();
            List<Pod> pods = client.pods().inNamespace(ns).withLabel("infinispan_cr", n).list().getItems();
            long running = pods.stream().map(p -> p.getStatus()).filter(s -> "Running".equalsIgnoreCase(s.getPhase())).count();

            out.printf("%-32s %-16s %-9s", n, ns, running + "/" + pods.size());
            if (secrets) {
               String secretName = Kube.getProperty(item, "spec", "security", "endpointSecretName");
               Secret secret = Kube.getSecret(client, ns, secretName);
               Kube.decodeOpaqueSecrets(secret).forEach(c -> out.printf("%n%-60s%-16s %-16s", "", c.username, c.password));
               out.println();
            } else {
               out.printf(" %-16s%n", "******");
            }
         });


         return CommandResult.SUCCESS;
      }
   }
}
