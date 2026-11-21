package org.infinispan.cli.graalvm;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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
import org.jboss.jandex.CompositeIndex;
import org.jboss.jandex.IndexView;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import io.fabric8.kubernetes.api.builder.Builder;
import io.fabric8.kubernetes.api.model.KubernetesResource;
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

   /**
    * Every Kubernetes model jar declares the resources it contributes in this service file, so its presence
    * identifies the jars holding the classes that Jackson (de)serializes.
    */
   private static final String MODEL_SERVICE_FILE = "META-INF/services/io.fabric8.kubernetes.api.model.KubernetesResource";

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
      ReflectionProcessor processor = new ReflectionProcessor(featureAccess, createIndex());
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

   /**
    * Indexes every Kubernetes jar that contributes classes Jackson reflects over. The model is split across one jar
    * per API group, so indexing only the jar of a single resource would hide the implementors and the annotations of
    * all the other groups, and the native image would fail to deserialize them.
    */
   private IndexView createIndex() {
      List<IndexView> indexes = new ArrayList<>();
      for (File jar : modelJars())
         indexes.add(Jandex.createIndex(jar));

      // Neither of these declares a resource: kubernetes-model-common holds the base classes of the model and
      // kubernetes-client-api the remaining payloads, e.g. VersionInfo and the OpenID Connect tokens
      indexes.add(Jandex.createIndex(Builder.class, KubernetesClient.class));
      return CompositeIndex.create(indexes);
   }

   private Collection<File> modelJars() {
      Set<File> jars = new LinkedHashSet<>();
      try {
         Enumeration<URL> services = featureAccess.getApplicationClassLoader().getResources(MODEL_SERVICE_FILE);
         while (services.hasMoreElements()) {
            File jar = jarOf(services.nextElement());
            if (jar != null)
               jars.add(jar);
         }
      } catch (IOException e) {
         throw new UncheckedIOException("Unable to enumerate the Kubernetes model jars", e);
      }

      if (jars.isEmpty())
         throw new IllegalStateException("No Kubernetes model jar on the classpath: " + MODEL_SERVICE_FILE + " not found");

      return jars;
   }

   private static File jarOf(URL resource) {
      // Jandex indexes jars only. A model loaded from an exploded directory is not something this build produces
      if (!"jar".equals(resource.getProtocol()))
         return null;

      try {
         URL jar = ((JarURLConnection) resource.openConnection()).getJarFileURL();
         return new File(jar.toURI());
      } catch (IOException e) {
         throw new UncheckedIOException(String.format("Unable to resolve the jar of '%s'", resource), e);
      } catch (URISyntaxException e) {
         throw new IllegalArgumentException(String.format("Unable to resolve the jar of '%s'", resource), e);
      }
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
