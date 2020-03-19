package org.infinispan.cli.commands.kubernetes;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Option;
import org.aesh.command.shell.Shell;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Ls.CMD, description = "Lists resources in a path")
public class Ls extends CliCommand {
   public static final String CMD = "ls";

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
      ServiceList list = kubernetesClient.services().inNamespace(namespace).withLabel("app", Kube.INFINISPAN_SERVICE_LABEL).list();
      Shell shell = invocation.getShell();
      shell.writeln("NAME                                   ADDRESS             ");
      for(Service item : list.getItems()) {
         ObjectMeta metadata = item.getMetadata();
         shell.writeln(String.format("%-48s %s:%d", item.getMetadata().getName(), item.getSpec().getClusterIP(), item.getSpec().getPorts().get(0).getPort()));
      }

      //kubernetesClient.apps().replicaSets().list().getItems().forEach(i -> System.out.println(i));

      return CommandResult.SUCCESS;
   }
}
