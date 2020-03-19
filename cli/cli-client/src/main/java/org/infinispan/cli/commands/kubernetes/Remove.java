package org.infinispan.cli.commands.kubernetes;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Remove.CMD, description = "Removes a service")
public class Remove extends CliCommand {
   public static final String CMD = "remove";

   @Option(shortName = 'n', description = "Select the namespace", defaultValue = "default")
   String namespace;

   @Argument(required = true, description = "The name of the service to remove")
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
      Boolean deleted = kubernetesClient.services().inNamespace(namespace).withName(name).delete();
      return deleted ? CommandResult.SUCCESS : CommandResult.FAILURE;
   }
}
