package org.infinispan.cli.commands.kubernetes;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceStatus;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Create.CMD, description = "Create a service")
public class Create extends CliCommand {
   public static final String CMD = "create";

   @Option(shortName = 'n', description = "Select the namespace", defaultValue = Kube.DEFAULT_NAMESPACE)
   String namespace;

   @Option(shortName = 'r', description = "The number of replicas", defaultValue = "1")
   int replicas;

   @Argument(description = "The name of the service to create", defaultValue = Kube.DEFAULT_SERVICE_NAME)
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

      Service service = kubernetesClient.services().inNamespace(namespace).createNew()
            .withKind("Infinispan")
            .withApiVersion(Kube.INFINISPAN_API_V1)
            .withNewMetadata()
            .withName(name)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .done();
      ServiceStatus status = service.getStatus();


      return CommandResult.SUCCESS;
   }
}
