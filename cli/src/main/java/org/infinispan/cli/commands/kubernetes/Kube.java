package org.infinispan.cli.commands.kubernetes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.aesh.command.Command;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.invocation.CommandInvocation;
import org.infinispan.cli.commands.Version;
import org.infinispan.cli.logging.Messages;
import org.yaml.snakeyaml.Yaml;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@GroupCommandDefinition(
      name = "kube",
      description = "Kubernetes commands",
      groupCommands = {
            Create.class,
            Install.class,
            Shell.class,
            Get.class,
            Delete.class,
            Uninstall.class,
            Version.class
      })
public class Kube implements Command {
   public static final String DEFAULT_CLUSTER_NAME = "infinispan";
   public static final String INFINISPAN_SERVICE_LABEL = "infinispan-service";

   static final CustomResourceDefinitionContext INFINISPAN_CLUSTER_CRD = new CustomResourceDefinitionContext.Builder()
         .withName("infinispans.infinispan.org")
         .withGroup("infinispan.org")
         .withKind("Infinispan")
         .withPlural("infinispans")
         .withScope("Namespaced")
         .withVersion("v1")
         .build();

   static final CustomResourceDefinitionContext OPERATOR_SUBSCRIPTION_CRD = new CustomResourceDefinitionContext.Builder()
         .withGroup("operators.coreos.com")
         .withKind("Subscription")
         .withPlural("subscriptions")
         .withScope("Namespaced")
         .withVersion("v1alpha1")
         .build();

   static final CustomResourceDefinitionContext OPERATOR_OPERATORGROUP_CRD = new CustomResourceDefinitionContext.Builder()
         .withGroup("operators.coreos.com")
         .withKind("OperatorGroup")
         .withPlural("operatorgroups")
         .withScope("Namespaced")
         .withVersion("v1")
         .build();

   @Override
   public CommandResult execute(CommandInvocation invocation) {
      invocation.getShell().write(invocation.getHelpInfo());
      return CommandResult.FAILURE;
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

   static Secret getSecret(KubernetesClient client, String namespace, String name) {
      Secret secret = client.secrets().inNamespace(namespace).withName(name).get();
      if (secret == null) {
         throw Messages.MSG.noGeneratedSecret(name);
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
      for(Map<String, String> credentials : credentialsList) {
         consumer.accept(credentials.get("username"), credentials.get("password"));
      }
   }

   static Secret getCertificateSecret(KubernetesClient client, Service service) {
      Secret secret = client.secrets().inNamespace(service.getMetadata().getNamespace()).withName(service.getMetadata().getName() + "-cert-secret").get();
      if (secret == null) {
         throw Messages.MSG.noGeneratedSecret(service.getMetadata().getName());
      } else {
         return secret;
      }
   }

   static String getNamespaceOrDefault(KubernetesClient client, String namespace) {
      String ns = namespace != null ? namespace : client.getConfiguration().getNamespace();
      if (ns == null) {
         throw Messages.MSG.noDefaultNamespace();
      }
      return ns;
   }

   static String decodeCertificateSecret(Secret secret) {
      String opaqueCertificate = secret.getData().get("tls.crt");
      return new String(Base64.getDecoder().decode(opaqueCertificate));
   }

   static String loadResourceAsString(String name, Properties replacements) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(Kube.class.getResourceAsStream(name)))) {
         String resource = reader.lines().collect(Collectors.joining(System.lineSeparator()));
         for(String propertyName : replacements.stringPropertyNames()) {
            resource = resource.replaceAll("\\{\\{" + propertyName + "\\}\\}", replacements.getProperty(propertyName));
         }
         return resource;
      } catch (IOException e) {
         throw Messages.MSG.genericError(name, e);
      }
   }

   static void add(Map<String, Object> map, String key, Object value) {
      String[] parts = key.split("\\.");
      for (int i = 0; i < parts.length - 1; i++) {
         if (map.containsKey(parts[i])) {
            map = (Map<String, Object>) map.get(parts[i]);
         } else {
            Map<String, Object> sub = new HashMap<>();
            map.put(parts[i], sub);
            map = sub;
         }
      }
      map.put(parts[parts.length - 1], value);
   }

   static <T> T get(Map<String, Object> map, String key) {
      String[] parts = key.split("\\.");
      for (int i = 0; i < parts.length - 1; i++) {
         if (map.containsKey(parts[i])) {
            map = (Map<String, Object>) map.get(parts[i]);
         } else {
            return null;
         }
      }
      return (T) map.get(parts[parts.length - 1]);
   }
}
