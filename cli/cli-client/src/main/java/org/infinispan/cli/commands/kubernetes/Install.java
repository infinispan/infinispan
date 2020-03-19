package org.infinispan.cli.commands.kubernetes;

import java.io.IOException;
import java.io.InputStream;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.kohsuke.MetaInfServices;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Install.CMD, description = "Installs the Infinispan operator")
public class Install extends CliCommand {
   public static final String CMD = "install";

   @Option(shortName = 'n', description = "Select the namespace", defaultValue = "default")
   String namespace;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      KubernetesClient kubernetesClient = invocation.getContext().getKubernetesClient();

      try(InputStream crd = this.getClass().getClassLoader().getResourceAsStream("operator/rbac.yaml")) {
         kubernetesClient.load(crd).inNamespace(namespace).createOrReplace();
      } catch (IOException e) {
         throw Messages.MSG.genericError("rbac.yaml", e);
      }

      try(InputStream crd = this.getClass().getClassLoader().getResourceAsStream("operator/crd.yaml")) {
         kubernetesClient.load(crd).inNamespace(namespace).createOrReplace();
      } catch (IOException e) {
         throw Messages.MSG.genericError("crd.yaml", e);
      }

      return CommandResult.SUCCESS;
   }
}
