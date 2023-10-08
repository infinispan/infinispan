package org.infinispan.quarkus.server.deployment;

import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.Arrays;
import java.util.Collection;

import org.infinispan.anchored.configuration.AnchoredKeysConfigurationBuilder;
import org.infinispan.commons.util.JVMMemoryInfoInfo;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.counter.configuration.CounterManagerConfigurationBuilder;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.quarkus.embedded.deployment.InfinispanReflectionExcludedBuildItem;
import org.infinispan.rest.RestServer;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespServer;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.tasks.Task;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.wildfly.security.password.impl.PasswordFactorySpiImpl;

import com.thoughtworks.xstream.security.NoTypePermission;

import io.netty.handler.codec.http2.CleartextHttp2ServerUpgradeHandler;
import io.netty.handler.codec.http2.Http2ServerUpgradeCodec;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.ExtensionSslNativeSupportBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.IndexDependencyBuildItem;
import io.quarkus.deployment.builditem.SystemPropertyBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ExcludeConfigBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageSystemPropertyBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.RuntimeInitializedClassBuildItem;

class InfinispanServerProcessor {
   private static final String FEATURE_NAME = "infinispan-server";
   @BuildStep
   void setSystemProperties(BuildProducer<NativeImageSystemPropertyBuildItem> buildSystemProperties,
                            BuildProducer<SystemPropertyBuildItem> systemProperties) {
      // We disable the replacement of JdkSslContext in the NettyExtensions - this shouldn't be needed once we move to Java 11
      buildSystemProperties.produce(new NativeImageSystemPropertyBuildItem("substratevm.replacement.jdksslcontext", "false"));
      // Make sure to disable the logging endpoint in JVM mode as it won't work as Quarkus replaces log4j classes
      systemProperties.produce(new SystemPropertyBuildItem("infinispan.server.resource.logging", "false"));
   }

   @BuildStep
   void extensionFeatureStuff(BuildProducer<FeatureBuildItem> feature, BuildProducer<AdditionalBeanBuildItem> additionalBeans,
                              BuildProducer<IndexDependencyBuildItem> indexedDependencies, BuildProducer<ExtensionSslNativeSupportBuildItem> sslNativeSupport) {
      feature.produce(new FeatureBuildItem(FEATURE_NAME));
      sslNativeSupport.produce(new ExtensionSslNativeSupportBuildItem(FEATURE_NAME));

      for (String infinispanArtifact : Arrays.asList(
            "infinispan-server-runtime",
            "infinispan-server-hotrod",
            "infinispan-server-core",
            "infinispan-server-rest",
            "infinispan-server-memcached",
            "infinispan-server-router",
            // Why is client-hotrod in dependency tree??
            "infinispan-client-hotrod",
            "infinispan-cachestore-jdbc",
            "infinispan-cachestore-remote",
            "infinispan-clustered-counter"
      )) {
         indexedDependencies.produce(new IndexDependencyBuildItem("org.infinispan", infinispanArtifact));
      }
   }

   @BuildStep
   void addExcludedClassesFromReflection(BuildProducer<InfinispanReflectionExcludedBuildItem> excludedClasses) {
      // We don't support Indexing so don't these to reflection
//      excludedClasses.produce(new InfinispanReflectionExcludedBuildItem(DotName.createSimple(AffinityIndexManager.class.getName())));
//      excludedClasses.produce(new InfinispanReflectionExcludedBuildItem(DotName.createSimple(ShardAllocationManagerImpl.class.getName())));
      // This class is used by JBossMarshalling so we don't need
      excludedClasses.produce(new InfinispanReflectionExcludedBuildItem(DotName.createSimple("org.infinispan.persistence.remote.upgrade.MigrationTask$RemoveListener")));

      // TODO: exclude all the TerminalFunctions SerializeWith references
   }

   @BuildStep
   void addRuntimeInitializedClasses(BuildProducer<RuntimeInitializedClassBuildItem> runtimeInitialized) {
      runtimeInitialized.produce(new RuntimeInitializedClassBuildItem(CleartextHttp2ServerUpgradeHandler.class.getName()));
      runtimeInitialized.produce(new RuntimeInitializedClassBuildItem(Http2ServerUpgradeCodec.class.getName()));
      runtimeInitialized.produce(new RuntimeInitializedClassBuildItem(Resp3Handler.class.getName()));
   }

   @BuildStep
   void addReflectionAndResources(BuildProducer<ReflectiveClassBuildItem> reflectionClass,
         BuildProducer<NativeImageResourceBuildItem> resources, CombinedIndexBuildItem combinedIndexBuildItem) {

      reflectionClass.produce(ReflectiveClassBuildItem.builder(PrivateGlobalConfigurationBuilder.class.getName()).build());
      reflectionClass.produce(ReflectiveClassBuildItem.builder(ServerConfigurationBuilder.class.getName()).build());
      // Add the various protocol server implementations
     reflectionClass.produce(ReflectiveClassBuildItem.builder(HotRodServer.class).build());
     reflectionClass.produce(ReflectiveClassBuildItem.builder(MemcachedServer.class).build());
     reflectionClass.produce(ReflectiveClassBuildItem.builder(RestServer.class).build());
     reflectionClass.produce(ReflectiveClassBuildItem.builder(RespServer.class).build());

      // We instantiate this during logging initialization
      reflectionClass.produce(ReflectiveClassBuildItem.builder("org.apache.logging.log4j.message.ReusableMessageFactory").build());
      reflectionClass.produce(ReflectiveClassBuildItem.builder("org.apache.logging.log4j.message.DefaultFlowMessageFactory").build());
      reflectionClass.produce(ReflectiveClassBuildItem.builder(PasswordFactorySpiImpl.class).build());

      IndexView combinedIndex = combinedIndexBuildItem.getIndex();
      addReflectionForClass(ProtocolServerConfigurationBuilder.class, false, combinedIndex, reflectionClass);

      // TODO: not sure why this is required for native runtime...
      reflectionClass.produce(ReflectiveClassBuildItem.builder(NoTypePermission.class.getName()).build());

      resources.produce(new NativeImageResourceBuildItem("infinispan-server-templates.xml",
            "proto/generated/global.core.proto",
            "proto/generated/global.counters.proto",
            "proto/generated/global.multimap.proto",
            "proto/generated/global.objectfilter.proto",
            "proto/generated/global.query.proto",
            "proto/generated/global.query.core.proto",
            "proto/generated/global.remote.query.server.proto",
            "proto/generated/global.remote.store.proto",
            "proto/generated/global.resp.proto",
            "proto/generated/global.scripting.proto",
            "proto/generated/global.server.core.proto",
            "proto/generated/global.server.hotrod.proto",
            "proto/generated/global.server.runtime.proto",
            "proto/generated/global.tasks.manager.proto",

            "proto/generated/persistence.counters.proto",
            "proto/generated/persistence.query.proto",
            "proto/generated/persistence.query.core.proto",
            "proto/generated/persistence.remote_query.proto",
            "proto/generated/persistence.memcached.proto",
            "proto/generated/persistence.resp.proto",
            "proto/generated/persistence.event_logger.proto",
            "proto/generated/persistence.multimap.proto",
            "proto/persistence.m.event_logger.proto",
            "proto/generated/persistence.server.core.proto",
            "proto/generated/persistence.servertasks.proto",
            "proto/generated/persistence.scripting.proto",
            "proto/generated/persistence.server_state.proto",
            "proto/generated/persistence.distribution.proto",
            "org/infinispan/query/remote/client/query.proto",
            WrappedMessage.PROTO_FILE
      ));

      // Add various classes required by the REST resources
      registerClass(reflectionClass, JVMMemoryInfoInfo.class, true, false, false);
      registerClass(reflectionClass, MemoryType.class, true, false, true);
      registerClass(reflectionClass, MemoryUsage.class, true, false, true);
      registerClass(reflectionClass, CacheManagerInfo.class, true, false, false);
      addReflectionForName(Task.class.getName(), true, combinedIndex, reflectionClass, true, false);

      reflectionClass.produce(ReflectiveClassBuildItem.builder("org.infinispan.health.impl.CacheHealthImpl").methods().build());
      reflectionClass.produce(ReflectiveClassBuildItem.builder("org.infinispan.health.impl.ClusterHealthImpl").methods().build());
      reflectionClass.produce(ReflectiveClassBuildItem.builder("org.infinispan.rest.resources.CacheManagerResource$HealthInfo").methods().build());

      reflectionClass.produce(ReflectiveClassBuildItem.builder("org.infinispan.rest.resources.CacheManagerResource$NamedCacheConfiguration").methods().build());
      reflectionClass.produce(ReflectiveClassBuildItem.builder("org.infinispan.rest.resources.CacheManagerResource$CacheInfo").methods().fields().build());
      reflectionClass.produce(ReflectiveClassBuildItem.builder("org.infinispan.rest.resources.ProtobufResource$ProtoSchema").fields().build());
      reflectionClass.produce(ReflectiveClassBuildItem.builder("org.infinispan.rest.resources.ProtobufResource$ValidationError").fields().build());
      // Register various Elytron classes
      String[] elytronClasses = new String[]{
            "org.wildfly.security.http.digest.DigestMechanismFactory",
            "org.wildfly.security.http.basic.BasicMechanismFactory",
            "org.wildfly.security.password.impl.PasswordFactorySpiImpl",
            "org.wildfly.security.sasl.digest.DigestClientFactory",
            "org.wildfly.security.sasl.digest.DigestServerFactory",
            "org.wildfly.security.sasl.localuser.LocalUserClientFactory",
            "org.wildfly.security.sasl.localuser.LocalUserServerFactory",
            "org.wildfly.security.sasl.plain.PlainSaslClientFactory",
            "org.wildfly.security.sasl.plain.PlainSaslServerFactory",
            "org.wildfly.security.sasl.scram.ScramSaslClientFactory",
            "org.wildfly.security.sasl.scram.ScramSaslServerFactory",
            "org.wildfly.security.credential.KeyPairCredential",
            "org.wildfly.security.credential.PasswordCredential",
            "org.wildfly.security.credential.SecretKeyCredential",
            "org.wildfly.security.credential.X509CertificateChainPrivateCredential",
      };
      reflectionClass.produce(ReflectiveClassBuildItem.builder(elytronClasses).methods().build());
      reflectionClass.produce(ReflectiveClassBuildItem.builder(CounterManagerConfigurationBuilder.class).build());
      reflectionClass.produce(ReflectiveClassBuildItem.builder(AnchoredKeysConfigurationBuilder.class).build());
      reflectionClass.produce(ReflectiveClassBuildItem.builder(RespServerConfigurationBuilder.class).build());
   }

   @BuildStep
   void excludeResourcesFromDependencies(BuildProducer<ExcludeConfigBuildItem> excludeConfigBuildItemBuildProducer) {
      excludeConfigBuildItemBuildProducer.produce(new ExcludeConfigBuildItem("io\\.lettuce\\.lettuce-core-.+.jar", "/META-INF/native-image/io.lettuce/lettuce-core/reflect-config.json"));
   }

   private void addReflectionForClass(Class<?> classToUse, boolean isInterface, IndexView indexView,
         BuildProducer<ReflectiveClassBuildItem> reflectiveClass) {
      addReflectionForName(classToUse.getName(), isInterface, indexView, reflectiveClass, false, false);
   }

   private void addReflectionForName(String className, boolean isInterface, IndexView indexView,
         BuildProducer<ReflectiveClassBuildItem> reflectiveClass, boolean methods, boolean fields) {
      Collection<ClassInfo> classInfos;
      if (isInterface) {
         classInfos = indexView.getAllKnownImplementors(DotName.createSimple(className));
      } else {
         classInfos = indexView.getAllKnownSubclasses(DotName.createSimple(className));
      }

      if (!classInfos.isEmpty()) {
         reflectiveClass.produce(ReflectiveClassBuildItem.builder(classInfos.stream().map(ClassInfo::toString).toArray(String[]::new))
                 .methods(methods).fields(fields).build());
      }
   }

   private void registerClass(BuildProducer<ReflectiveClassBuildItem> reflectionClass, Class<?> clazz, boolean methods,
                              boolean fields, boolean ignoreNested) {
      reflectionClass.produce(ReflectiveClassBuildItem.builder(clazz).methods(methods).fields(fields).build());
      if (!ignoreNested) {
         Class<?>[] declaredClasses = clazz.getDeclaredClasses();
         for (Class<?> declaredClass : declaredClasses)
            registerClass(reflectionClass, declaredClass, methods, fields, false);
      }
   }
}
