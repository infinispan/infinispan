package org.infinispan.client.hotrod.graalvm;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Stream;

import org.graalvm.nativeimage.hosted.Feature;
import org.infinispan.commons.graalvm.Bundle;
import org.infinispan.commons.graalvm.ClassLoaderFeatureAccess;
import org.infinispan.commons.graalvm.Jandex;
import org.infinispan.commons.graalvm.ReflectionProcessor;
import org.infinispan.commons.graalvm.ReflectiveClass;
import org.infinispan.commons.graalvm.Resource;
import org.jboss.jandex.IndexView;

public class NativeMetadataProvider implements org.infinispan.commons.graalvm.NativeMetadataProvider {

   static final Collection<Resource> resourceFiles = Resource.of(
         "org/infinispan/protostream/message-wrapping\\.proto",
         "org/infinispan/query/remote/client/query\\.proto",
         "protostream/common-java-types\\.proto",
         "protostream/common-java-container-types\\.proto"
   );

   static final Collection<Resource> resourceRegexps = Collections.emptyList();

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
      return Stream.of(
            resourceFiles.stream(),
            resourceRegexps.stream()
      ).flatMap(Function.identity());
   }

   @Override
   public Stream<Bundle> bundles() {
      return bundles.stream();
   }

   private ReflectionProcessor reflectionProcessor() {
      IndexView index = Jandex.createIndex(
            org.infinispan.protostream.GeneratedSchema.class, // ProtoStream
            org.infinispan.commons.CacheException.class, // Commons
            org.infinispan.client.hotrod.RemoteCache.class, // Client
            org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.class, // Remote Query
            org.infinispan.query.api.continuous.ContinuousQuery.class // Query DSL
      );
      return new ReflectionProcessor(featureAccess, index)
            .addImplementations(false, false,
                  org.jboss.logging.BasicLogger.class,
                  org.infinispan.commons.executors.ExecutorFactory.class
            ).addClasses(
                  org.infinispan.commons.jmx.PlatformMBeanServerLookup.class,
                  org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash.class
            ).addClasses(
                  "org.wildfly.security.sasl.plain.WildFlyElytronSaslPlainProvider",
                  "org.wildfly.security.sasl.digest.WildFlyElytronSaslDigestProvider",
                  "org.wildfly.security.sasl.external.WildFlyElytronSaslExternalProvider",
                  "org.wildfly.security.sasl.oauth2.WildFlyElytronSaslOAuth2Provider",
                  "org.wildfly.security.sasl.scram.WildFlyElytronSaslScramProvider",
                  "org.wildfly.security.sasl.gssapi.WildFlyElytronSaslGssapiProvider",
                  "org.wildfly.security.sasl.gs2.WildFlyElytronSaslGs2Provider",

                  "org.infinispan.client.hotrod.event.impl.ContinuousQueryImpl$ClientEntryListener",
                  "org.infinispan.client.hotrod.near.NearCacheService$InvalidatedNearCacheListener"
            );
   }
}
