package org.infinispan.cli.graalvm;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

import org.graalvm.nativeimage.hosted.Feature;
import org.infinispan.commons.graalvm.Bundle;
import org.infinispan.commons.graalvm.ClassLoaderFeatureAccess;
import org.infinispan.commons.graalvm.Jandex;
import org.infinispan.commons.graalvm.ReflectionProcessor;
import org.infinispan.commons.graalvm.ReflectiveClass;
import org.infinispan.commons.graalvm.Resource;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.IndexView;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import io.fabric8.kubernetes.api.builder.Builder;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * Native image metadata for the Kubernetes commands of the CLI, i.e. the ones used when the CLI is installed as a
 * <code>kubectl</code>/<code>oc</code> plugin.
 * <p>
 * The Kubernetes client resolves its implementation by name and (de)serializes the Kubernetes model with Jackson, so
 * none of it is reachable by static analysis.
 *
 * @since 16.3
 */
public class NativeMetadataProvider implements org.infinispan.commons.graalvm.NativeMetadataProvider {

   private static final String FABRIC8_PACKAGE_PREFIX = "io.fabric8.";

   static final Collection<Resource> resourceFiles = Resource.of(
         "META-INF/services/io\\.fabric8\\..*"
   );

   static final Collection<Bundle> bundles = Collections.emptyList();

   final Feature.FeatureAccess featureAccess;
   final ReflectionProcessor reflection;

   public NativeMetadataProvider() {
      this(new ClassLoaderFeatureAccess(NativeMetadataProvider.class.getClassLoader()));
   }

   public NativeMetadataProvider(Feature.FeatureAccess featureAccess) {
      this.featureAccess = featureAccess;
      this.reflection = reflectionProcessor();
   }

   @Override
   public Stream<ReflectiveClass> reflectiveClasses() {
      return reflection.classes();
   }

   @Override
   public Stream<Resource> includedResources() {
      return resourceFiles.stream();
   }

   @Override
   public Stream<Bundle> bundles() {
      return bundles.stream();
   }

   private ReflectionProcessor reflectionProcessor() {
      IndexView index = Jandex.createIndex(
            Pod.class, // kubernetes-model-core, which also holds the kubeconfig model
            Builder.class, // kubernetes-model-common
            KubernetesClient.class // kubernetes-client-api
      );
      ReflectionProcessor processor = new ReflectionProcessor(featureAccess, index);
      return processor
            // Jackson needs the accessors of every model class it (de)serializes, including the kubeconfig ones
            .addImplementations(true, true, KubernetesResource.class)
            // The remaining payloads of the client, e.g. VersionInfo and the OpenID Connect tokens
            .addClassesWithAnnotation(true, true, JsonIgnoreProperties.class)
            .addClasses(
                  // KubernetesClientBuilder loads the implementation by name and invokes its constructor reflectively
                  "io.fabric8.kubernetes.client.impl.KubernetesClientImpl"
            )
            // Jackson instantiates the (de)serializers referenced by these annotations reflectively
            .forEachAnnotation(JsonDeserialize.class, instance -> addReferencedClasses(processor, instance))
            .forEachAnnotation(JsonSerialize.class, instance -> addReferencedClasses(processor, instance));
   }

   private static void addReferencedClasses(ReflectionProcessor processor, AnnotationInstance instance) {
      for (AnnotationValue value : instance.values()) {
         if (value.kind() != AnnotationValue.Kind.CLASS)
            continue;

         // Anything outside of the Kubernetes model is a Jackson placeholder such as JsonDeserializer.None
         String className = value.asClass().name().toString();
         if (className.startsWith(FABRIC8_PACKAGE_PREFIX))
            processor.addClasses(className);
      }
   }
}
