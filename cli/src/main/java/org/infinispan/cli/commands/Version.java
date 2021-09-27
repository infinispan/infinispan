package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Option;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.KubernetesContext;
import org.kohsuke.MetaInfServices;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "version", description = "Shows version information")
public class Version extends CliCommand {

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      invocation.println(String.format("CLI: %s", org.infinispan.commons.util.Version.printVersion()));
      if (invocation.getContext().isConnected()) {
         invocation.println("Server: " + invocation.getContext().getConnection().getServerVersion());
      }
      if (invocation.getContext() instanceof KubernetesContext) {
         KubernetesClient client = ((KubernetesContext) invocation.getContext()).getKubernetesClient();
         invocation.printf("Kubernetes %s.%s\n", client.getVersion().getMajor(), client.getVersion().getMinor());
      }
      return CommandResult.SUCCESS;
   }
}
