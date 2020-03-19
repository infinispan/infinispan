package org.infinispan.cli.commands.kubernetes;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Interactive.CMD, description = "Interactive CLI")
public class Interactive extends CliCommand {
   public static final String CMD = "interactive";

   @Option(shortName = 'u', description = "The username to use when connecting. If not specified, the secret associated with the service will be used")
   String username;

   @Option(shortName = 'p', description = "The password to use when connecting. If not specified, the secret associated with the service will be used")
   String password;

   @Option(shortName = 'n', description = "Select the namespace", defaultValue = "default")
   String namespace;

   @Argument(description = "The name of the service to connect to.")
   String name;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      KubernetesClient kubernetesClient = invocation.getContext().getKubernetesClient();

      Service service = Kube.getService(kubernetesClient, namespace, name);
      name = service.getMetadata().getName();

      // Obtain the secret for the service
      if (username == null && password == null) {
         Secret secret = Kube.getGeneratedSecret(kubernetesClient, service);
         Kube.decodeIdentitiesSecret(secret, (u, p) -> {
            username = u;
            password = p;
         });
      }

      return CommandResult.SUCCESS;
   }
}
