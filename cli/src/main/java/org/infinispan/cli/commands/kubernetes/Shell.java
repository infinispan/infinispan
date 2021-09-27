package org.infinispan.cli.commands.kubernetes;

import static org.infinispan.cli.commands.kubernetes.Kube.DEFAULT_CLUSTER_NAME;
import static org.infinispan.cli.commands.kubernetes.Kube.INFINISPAN_CLUSTER_CRD;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.DefaultShell;
import org.infinispan.cli.impl.KubernetesContext;
import org.infinispan.cli.logging.Messages;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.LocalPortForward;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@CommandDefinition(name = "shell", description = "Initiates an interactive shell with a service.")
public class Shell extends CliCommand {

   @Option(shortName = 'n', description = "Specifies the namespace where the cluster is running. Uses the default namespace if you do not specify one.")
   String namespace;

   @Option(shortName = 'p', name = "pod-name", description = "Specifies to which pod you connect.")
   String podName;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Argument(description = "Specifies the name of the service to connect to. Defaults to '" + DEFAULT_CLUSTER_NAME + "'", defaultValue = DEFAULT_CLUSTER_NAME)
   String name;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      KubernetesClient client = KubernetesContext.getClient(invocation);
      namespace = Kube.getNamespaceOrDefault(client, namespace);
      GenericKubernetesResource infinispan = client.genericKubernetesResources(INFINISPAN_CLUSTER_CRD).inNamespace(namespace).withName(name).get();
      if (infinispan == null) {
         throw Messages.MSG.noSuchService(name, namespace);
      }
      String endpointSecretName = Kube.getProperty(infinispan, "spec", "security", "endpointSecretName");
      String certSecretName = Kube.getProperty(infinispan, "spec", "security", "endpointEncryption", "certSecretName");

      Pod pod;
      if (podName == null) {
         pod = client.pods().inNamespace(namespace).withLabel("infinispan_cr", name).list()
               .getItems().stream().filter(p -> "running".equalsIgnoreCase(p.getStatus().getPhase())).findFirst().orElse(null);
      } else {
         pod = client.pods().inNamespace(namespace).withName(podName).get();
      }
      if (pod == null) {
         throw Messages.MSG.noRunningPodsInService(name);
      }
      // Port forwarding mode
      List<ContainerPort> ports = pod.getSpec().getContainers().get(0).getPorts();
      // Find the `infinispan` port
      ContainerPort containerPort = ports.stream().filter(p -> "infinispan".equals(p.getName())).findFirst().get();
      try (LocalPortForward portForward = client.pods().inNamespace(namespace).withName(pod.getMetadata().getName()).portForward(containerPort.getContainerPort())) {
         StringBuilder connection = new StringBuilder();
         List<String> args = new ArrayList<>();
         if (certSecretName != null) {
            connection.append("https://");
            Secret secret = Kube.getSecret(client, namespace, certSecretName);
            String crt = secret.getData().get("tls.crt");
            String pem = new String(Base64.getDecoder().decode(crt));
            Path path = Files.createTempFile("clitrust", "pem", PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            Files.write(path, pem.getBytes(StandardCharsets.UTF_8));
            args.add("-t");
            args.add(path.toString());
         } else {
            connection.append("http://");
         }
         if (endpointSecretName != null) {
            Secret secret = Kube.getSecret(client, namespace, endpointSecretName);
            List<Kube.SecretCredentials> credentials = Kube.decodeOpaqueSecrets(secret);
            connection.append(credentials.get(0).username);
            connection.append(':');
            connection.append(credentials.get(0).password);
            connection.append('@');
         }
         InetAddress localAddress = portForward.getLocalAddress();
         if (localAddress.getAddress().length == 4) {
            connection.append(localAddress.getHostAddress());
         } else {
            connection.append('[').append(localAddress.getHostAddress()).append(']');
         }
         connection.append(':');
         connection.append(portForward.getLocalPort());
         args.add("-c");
         args.add(connection.toString());
         CLI.main(new DefaultShell(), args.toArray(new String[0]), System.getProperties(), false);
         return CommandResult.SUCCESS;
      } catch (Throwable t) {
         return CommandResult.FAILURE;
      }
   }
}
