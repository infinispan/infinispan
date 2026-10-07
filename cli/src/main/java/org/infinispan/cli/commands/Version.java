package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.KubernetesContext;
import org.infinispan.commons.util.Util;
import org.kohsuke.MetaInfServices;

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
      if (invocation.getContext().isConnected()) {
         invocation.println("Server: " + invocation.getContext().connection().getServerVersion());
      }
      if (invocation.getContext() instanceof KubernetesContext) {
         printKubernetesVersion(invocation, (KubernetesContext) invocation.getContext());
      }
      return CommandResult.SUCCESS;
   }

   /**
    * Reporting the version of the cluster is best-effort: the CLI version must be shown even when there is no
    * usable kubeconfig or the API server cannot be reached.
    */
   private static void printKubernetesVersion(ContextAwareCommandInvocation invocation, KubernetesContext context) {
      try {
         VersionInfo version = context.getKubernetesClient().getVersion();
         invocation.printf("Kubernetes %s.%s\n", version.getMajor(), version.getMinor());
      } catch (RuntimeException e) {
         Throwable cause = Util.getRootCause(e);
         invocation.printf("Kubernetes: unavailable (%s)\n", cause.getMessage() != null ? cause.getMessage() : cause.toString());
      }
   }
}
