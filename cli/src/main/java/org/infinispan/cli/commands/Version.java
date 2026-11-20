package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.KubernetesContext;
import org.kohsuke.MetaInfServices;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "version", description = "Shows version information")
public class Version extends CliCommand {

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      invocation.println(String.format("CLI: %s", org.infinispan.commons.util.Version.printVersion()));
      Connection connection = invocation.getContext().connection();
      if (connection.isConnected()) {
         invocation.println("Server: " + connection.getServerVersion());
      }
      if (invocation.getContext() instanceof KubernetesContext) {
         KubernetesClient client = ((KubernetesContext) invocation.getContext()).getKubernetesClient();
         VersionInfo kubernetesVersion = client.getKubernetesVersion();
         invocation.printf("Kubernetes %s.%s\n", kubernetesVersion.getMajor(), kubernetesVersion.getMinor());
      }
      return CommandResult.SUCCESS;
   }
}
