package org.infinispan.cli.commands.kubernetes;

import static org.infinispan.cli.commands.kubernetes.Kube.DEFAULT_CLUSTER_NAME;
import static org.infinispan.cli.commands.kubernetes.Kube.INFINISPAN_CLUSTER_CRD;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.EncryptionCompleter;
import org.infinispan.cli.completers.ExposeCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.KubernetesContext;
import org.infinispan.cli.logging.Messages;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/

@GroupCommandDefinition(
      name = "create",
      description = "Creates resources.",
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
      // This command serves only to wrap the sub-commands
      invocation.println(invocation.getHelpInfo());
      return CommandResult.FAILURE;
   }

   @CommandDefinition(name = "cluster", description = "Creates a cluster")
   public static class Cluster extends CliCommand {

      @Option(shortName = 'n', description = "Specifies the namespace where the cluster is created. Uses the default namespace if you do not specify one.")
      String namespace;

      @Option(shortName = 'r', description = "Specifies the number of replicas. Defaults to 1.", defaultValue = "1")
      int replicas;

      @Option(name = "expose-type", completer = ExposeCompleter.class, description = "Makes the service available on the network through a LoadBalancer, NodePort, or Route.")
      String exposeType;

      @Option(name = "expose-port", defaultValue = "0", description = "Sets the network port where the service is available. You must set a port if the expose type is LoadBalancer or NodePort.")
      int exposePort;

      @Option(name = "expose-host", description = "Optionally sets the hostname if the expose type is Route.")
      String exposeHost;

      @Option(name = "encryption-type", completer = EncryptionCompleter.class, description = "The type of encryption: one of None, Secret, Service")
      String encryptionType;

      @Option(name = "encryption-secret", description = "The name of the secret containing the TLS certificate")
      String encryptionSecret;

      @Argument(description = "Defines the cluster name. Defaults to '" + DEFAULT_CLUSTER_NAME + "'", defaultValue = DEFAULT_CLUSTER_NAME)
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
         namespace = Kube.getNamespaceOrDefault(client, namespace);
         GenericKubernetesResource infinispan = new GenericKubernetesResource();
         infinispan.setKind(INFINISPAN_CLUSTER_CRD.getKind());
         ObjectMeta metadata = new ObjectMeta();
         metadata.setName(name);
         metadata.setNamespace(namespace);
         infinispan.setMetadata(metadata);
         GenericKubernetesResource spec = new GenericKubernetesResource();
         infinispan.setAdditionalProperty("spec", spec);
         spec.setAdditionalProperty("replicas", replicas);
         if (exposeType != null) {
            GenericKubernetesResource expose = new GenericKubernetesResource();
            spec.setAdditionalProperty("expose", expose);
            expose.setAdditionalProperty("type", exposeType);
            switch (ExposeCompleter.Expose.valueOf(exposeType)) {
               case LoadBalancer:
                  if (exposePort == 0) {
                     throw Messages.MSG.exposeTypeRequiresPort(exposeType);
                  } else {
                     expose.setAdditionalProperty("port", exposePort);
                  }
                  break;
               case NodePort:
                  if (exposePort == 0) {
                     throw Messages.MSG.exposeTypeRequiresPort(exposeType);
                  } else {
                     expose.setAdditionalProperty("nodePort", exposePort);
                  }
                  break;
            }
            if (exposeHost != null) {
               expose.setAdditionalProperty("host", exposeHost);
            }
         }
         if (encryptionType != null) {
            GenericKubernetesResource security = new GenericKubernetesResource();
            spec.setAdditionalProperty("security", security);
            GenericKubernetesResource encryption = new GenericKubernetesResource();
            security.setAdditionalProperty("endpointEncryption", encryption);
            encryption.setAdditionalProperty("type", encryptionType);
            if (EncryptionCompleter.Encryption.valueOf(encryptionType) == EncryptionCompleter.Encryption.Secret) {
               if (encryptionSecret != null) {
                  encryption.setAdditionalProperty("certSecretName", encryptionSecret);
               } else {
                  throw Messages.MSG.encryptionTypeRequiresSecret(encryptionType);
               }
            }
         }
         client.genericKubernetesResources(INFINISPAN_CLUSTER_CRD).inNamespace(namespace).create(infinispan);
         return CommandResult.SUCCESS;
      }
   }
}
