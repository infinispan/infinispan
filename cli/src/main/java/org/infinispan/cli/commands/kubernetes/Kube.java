package org.infinispan.cli.commands.kubernetes;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.aesh.command.Command;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.invocation.CommandInvocation;
import org.infinispan.cli.commands.Version;
import org.infinispan.cli.logging.Messages;
import org.yaml.snakeyaml.Yaml;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@GroupCommandDefinition(
      name = "kube",
      description = "Kubernetes commands.",
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

   static final CustomResourceDefinitionContext INFINISPAN_CLUSTER_CRD = new CustomResourceDefinitionContext.Builder()
         .withName("infinispans.infinispan.org")
         .withGroup("infinispan.org")
         .withKind("Infinispan")
         .withPlural("infinispans")
         .withScope("Namespaced")
         .withVersion("v1")
         .build();

   static final CustomResourceDefinitionContext OPERATOR_CATALOGSOURCE_CRD = new CustomResourceDefinitionContext.Builder()
         .withGroup("operators.coreos.com")
         .withKind("CatalogSource")
         .withPlural("catalogsources")
         .withScope("Namespaced")
         .withVersion("v1alpha1")
         .build();

   static final CustomResourceDefinitionContext OPERATOR_CLUSTERSERVICEVERSION_CRD = new CustomResourceDefinitionContext.Builder()
         .withGroup("operators.coreos.com")
         .withKind("ClusterServiceVersion")
         .withPlural("clusterserviceversions")
         .withScope("Namespaced")
         .withVersion("v1alpha1")
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

   public static <T> T getProperty(GenericKubernetesResource item, String... names) {
      Map<String, Object> properties = item.getAdditionalProperties();
      for (int i = 0; i < names.length - 1; i++) {
         properties = (Map<String, Object>) properties.get(names[i]);
         if (properties == null) {
            return null;
         }
      }
      return (T) properties.get(names[names.length -1 ]);
   }


   @Override
   public CommandResult execute(CommandInvocation invocation) {
      invocation.getShell().write(invocation.getHelpInfo());
      return CommandResult.FAILURE;
   }

   static Secret getSecret(KubernetesClient client, String namespace, String name) {
      Secret secret = client.secrets().inNamespace(namespace).withName(name).get();
      if (secret == null) {
         throw Messages.MSG.noGeneratedSecret(name);
      } else {
         return secret;
      }
   }

   static List<SecretCredentials> decodeOpaqueSecrets(Secret secret) {
      String opaqueIdentities = secret.getData().get("identities.yaml");
      String identitiesYaml = new String(Base64.getDecoder().decode(opaqueIdentities));
      Yaml yaml = new Yaml();
      Map<String, List> identities = yaml.load(identitiesYaml);
      List<Map<String, String>> credentialsList = identities.get("credentials");
      List<SecretCredentials> res = new ArrayList<>(identities.size());
      for (Map<String, String> credentials : credentialsList) {
         res.add(new SecretCredentials(credentials.get("username"), credentials.get("password")));
      }
      return res;
   }


   static String getNamespaceOrDefault(KubernetesClient client, String namespace) {
      String ns = namespace != null ? namespace : client.getConfiguration().getNamespace();
      if (ns == null) {
         throw Messages.MSG.noDefaultNamespace();
      }
      return ns;
   }

   static String defaultOperatorNamespace(KubernetesClient client) {
      // Global installation: determine the namespace
      List<Namespace> namespaces = client.namespaces().list().getItems();
      Optional<Namespace> ns = namespaces.stream().filter(n -> "openshift-operators".equals(n.getMetadata().getName())).findFirst();
      if (!ns.isPresent()) {
         ns = namespaces.stream().filter(n -> "operators".equals(n.getMetadata().getName())).findFirst();
      }
      if (ns.isPresent()) {
         return ns.get().getMetadata().getName();
      } else {
         throw Messages.MSG.noDefaultOperatorNamespace();
      }
   }

   public static class SecretCredentials {
      String username;
      String password;

      public SecretCredentials(String username, String password) {
         this.username = username;
         this.password = password;
      }
   }
}
