package org.infinispan.cli.commands.kubernetes;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.aesh.command.Command;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.invocation.CommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.yaml.snakeyaml.Yaml;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@GroupCommandDefinition(
      name = Kube.CMD,
      description = "",
      groupCommands = {
            Install.class,
            Interactive.class,
            Create.class,
            Ls.class,
            Remove.class
      })
public class Kube implements Command {
   public static final String CMD = "kube";
   public static final String DEFAULT_NAMESPACE = "default";
   public static final String DEFAULT_SERVICE_NAME = "infinispan";
   public static final String INFINISPAN_API_V1 = "infinispan.org/v1";

   static final String INFINISPAN_SERVICE_LABEL = "infinispan-service";


   @Override
   public CommandResult execute(CommandInvocation invocation) {
      return CommandResult.SUCCESS;
   }


   // Utility methods used by the various commands
   static Service getService(KubernetesClient client, String namespace, String name) {
      if (name == null) {
         List<Service> services = client.services().withLabel("app", INFINISPAN_SERVICE_LABEL).list().getItems();
         if (services.isEmpty()) {
            throw Messages.MSG.noServicesFound();
         }
         if (services.size() > 1) {
            throw Messages.MSG.specifyService();
         }
         return services.get(0);

      } else {
         Service service = client.services().inNamespace(namespace).withName(name).get();
         if (service == null) {
            throw Messages.MSG.noSuchService(name);
         } else if (!INFINISPAN_SERVICE_LABEL.equals(service.getMetadata().getLabels().get("app"))) {
            throw Messages.MSG.wrongServiceType(name);
         } else {
            return service;
         }
      }
   }

   static Secret getGeneratedSecret(KubernetesClient client, Service service) {
      Secret secret = client.secrets().inNamespace(service.getMetadata().getNamespace()).withName(service.getMetadata().getName() + "-generated-secret").get();
      if (secret == null) {
         throw Messages.MSG.noGeneratedSecret(service.getMetadata().getName());
      } else {
         return secret;
      }
   }

   static void decodeIdentitiesSecret(Secret secret, BiConsumer<String, String> consumer) {
      String opaqueIdentities = secret.getData().get("identities.yaml");
      String identitiesYaml = new String(Base64.getDecoder().decode(opaqueIdentities));
      Yaml yaml = new Yaml();
      Map<String, List> identities = (Map<String, List>) yaml.load(identitiesYaml);
      List<Map<String, String>> credentialsList = identities.get("credentials");
      Map<String, String> credentials = credentialsList.get(0);
      consumer.accept(credentials.get("username"), credentials.get("password"));
   }

   static Secret getCertificateSecret(KubernetesClient client, Service service) {
      Secret secret = client.secrets().inNamespace(service.getMetadata().getNamespace()).withName(service.getMetadata().getName() + "-cert-secret").get();
      if (secret == null) {
         throw Messages.MSG.noGeneratedSecret(service.getMetadata().getName());
      } else {
         return secret;
      }
   }

   static String decodeCertificateSecret(Secret secret) {
      String opaqueCertificate = secret.getData().get("tls.crt");
      return new String(Base64.getDecoder().decode(opaqueCertificate));
   }

}
