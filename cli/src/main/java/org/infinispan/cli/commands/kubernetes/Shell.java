package org.infinispan.cli.commands.kubernetes;

import static org.infinispan.cli.commands.kubernetes.Kube.DEFAULT_CLUSTER_NAME;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.KubernetesContextImpl;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import okhttp3.Response;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@CommandDefinition(name = "shell", description = "Initiates an interactive shell with a service")
public class Shell extends CliCommand {

   @Option(shortName = 'n', description = "Select the namespace")
   String namespace;

   @Option(description = "The name of the pod to connect to")
   String podName;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Argument(description = "The name of the service to connect to", defaultValue = DEFAULT_CLUSTER_NAME)
   String name;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      KubernetesClient client = ((KubernetesContextImpl)invocation.getContext()).getKubernetesClient();
      namespace = Kube.getNamespaceOrDefault(client, namespace);
      if (podName == null) {
         List<Pod> pods = client.pods().inNamespace(namespace).withLabel("infinispan_cr", name).list().getItems();
         for(Pod pod : pods) {
            PodStatus status = pod.getStatus();
            if ("Running".equalsIgnoreCase(status.getPhase())) {
               podName = pod.getMetadata().getName();
               break;
            }
         }
      }
      if (podName == null) {
         System.err.printf("No running pods available in service %s\n", name);
      }

      LatchListener latchListener = new LatchListener();
      client.pods().inNamespace(namespace).withName(podName)
            .readingInput(System.in)
            .writingOutput(System.out)
            .writingError(System.err)
            .withTTY()
            .usingListener(latchListener)
            .exec("/opt/infinispan/bin/cli.sh");

      try {
         latchListener.latch.await();
      } catch (InterruptedException e) {
      }
      return CommandResult.SUCCESS;
   }

   public static class LatchListener implements ExecListener {
      CountDownLatch latch = new CountDownLatch(1);

      @Override
      public void onOpen(Response response) {
      }

      @Override
      public void onFailure(Throwable t, Response response) {
         latch.countDown();
      }

      @Override
      public void onClose(int code, String reason) {
         latch.countDown();
      }
   }
}
