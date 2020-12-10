package org.infinispan.cli.commands.kubernetes;

import static org.infinispan.cli.commands.kubernetes.Kube.INFINISPAN_CLUSTER_CRD;
import static org.infinispan.cli.commands.kubernetes.Kube.get;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

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

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@GroupCommandDefinition(
      name = "get",
      description = "Displays resources",
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

      @Option(shortName = 'n', description = "Select the namespace")
      String namespace;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Option(shortName = 's', hasValue = false)
      protected boolean secrets;

      @Argument
      String name;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         KubernetesClient client = ((KubernetesContextImpl) invocation.getContext()).getKubernetesClient();
         namespace = Kube.getNamespaceOrDefault(client, namespace);
         Map<String, Object> resource = client.customResource(INFINISPAN_CLUSTER_CRD).list();
         List<Map<String, Object>> items = get(resource, "items");
         PrintStream out = invocation.getShellOutput();
         out.printf("%-32s %-16s %-9s %-16s%n", "NAME", "NAMESPACE", "STATUS", "SECRETS");
         items.forEach(item -> {
            String n = get(item, "metadata.name");
            String ns = get(item, "metadata.namespace");
            List<Pod> pods = client.pods().inNamespace(ns).withLabel("infinispan_cr", n).list().getItems();
            long running = pods.stream().map(p -> p.getStatus()).filter(s -> "Running".equalsIgnoreCase(s.getPhase())).count();

            out.printf("%-32s %-16s %-9s", n, ns, running + "/" + pods.size());
            if (secrets) {
               String secretName = get(item, "spec.security.endpointSecretName");
               Secret secret = Kube.getSecret(client, ns, secretName);
               Kube.decodeIdentitiesSecret(secret, (u, p) -> out.printf("%n%-60s%-16s %-16s", "", u, p));
               out.println();
            } else {
               out.printf(" %-16s%n", "******");
            }
         });


         return CommandResult.SUCCESS;
      }
   }
}
