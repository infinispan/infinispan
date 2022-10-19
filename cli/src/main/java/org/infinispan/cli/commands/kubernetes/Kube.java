package org.infinispan.cli.commands.kubernetes;

import java.io.StringReader;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.aesh.command.Command;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.invocation.CommandInvocation;
import org.infinispan.cli.commands.Version;
import org.infinispan.cli.logging.Messages;
import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.configuration.io.PropertyReplacer;
import org.infinispan.commons.configuration.io.URLConfigurationResourceResolver;
import org.infinispan.commons.configuration.io.yaml.YamlConfigurationReader;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
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
      return (T) properties.get(names[names.length - 1]);
   }

   public static void setProperty(GenericKubernetesResource item, String value, String... names) {
      for (int i = 0; i < names.length - 1; i++) {
         Map<String, Object> properties = item.getAdditionalProperties();
         if (properties.containsKey(names[i])) {
            item = (GenericKubernetesResource) properties.get(names[i]);
         } else {
            GenericKubernetesResource child = new GenericKubernetesResource();
            item.setAdditionalProperty(names[i], child);
            item = child;
         }
      }
      item.setAdditionalProperty(names[names.length - 1], value);
   }


   @Override
   public CommandResult execute(CommandInvocation invocation) {
      invocation.getShell().write(invocation.getHelpInfo());
      return CommandResult.FAILURE;
   }

   static Secret getSecret(KubernetesClient client, String namespace, String name) {
      try {
         return client.secrets().inNamespace(namespace).withName(name).get();
      } catch (KubernetesClientException e) {
         return null;
      }
   }

   static Map<String, String> decodeOpaqueSecrets(Secret secret) {
      if (secret == null) {
         return Collections.emptyMap();
      }
      String opaqueIdentities = secret.getData().get("identities.yaml");
      String yaml = new String(Base64.getDecoder().decode(opaqueIdentities));
      YamlConfigurationReader reader = new YamlConfigurationReader(new StringReader(yaml), new URLConfigurationResourceResolver(null), new Properties(), PropertyReplacer.DEFAULT, NamingStrategy.KEBAB_CASE);
      Map<String, Object> identities = reader.asMap();
      List<Map<String, String>> credentialsList = (List<Map<String, String>>) identities.get("credentials");
      Map<String, String> res = new LinkedHashMap<>(identities.size());
      for (Map<String, String> credentials : credentialsList) {
         res.put(credentials.get("username"), credentials.get("password"));
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
}
