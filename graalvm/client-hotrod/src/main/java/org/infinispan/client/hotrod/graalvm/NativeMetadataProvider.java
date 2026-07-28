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
         "org/infinispan/.*\\.proto",
         "org/infinispan/protostream/.*\\.proto"
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
            org.wildfly.security.WildFlyElytronBaseProvider.class // Elytron
      );
      return new ReflectionProcessor(featureAccess, index)
            .addImplementations(false, false,
                  org.jboss.logging.BasicLogger.class,
                  org.infinispan.commons.executors.ExecutorFactory.class
            ).addClasses(
                  org.infinispan.commons.jmx.PlatformMBeanServerLookup.class,
                  org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash.class
            ).addClasses(false, true,
                  "org.wildfly.security.sasl.plain.WildFlyElytronSaslPlainProvider",
                  "org.wildfly.security.sasl.digest.WildFlyElytronSaslDigestProvider",
                  "org.wildfly.security.sasl.external.WildFlyElytronSaslExternalProvider",
                  "org.wildfly.security.sasl.oauth2.WildFlyElytronSaslOAuth2Provider",
                  "org.wildfly.security.sasl.scram.WildFlyElytronSaslScramProvider",
                  "org.wildfly.security.sasl.gssapi.WildFlyElytronSaslGssapiProvider",
                  "org.wildfly.security.sasl.gs2.WildFlyElytronSaslGs2Provider",
                  "org.wildfly.security.password.WildFlyElytronPasswordProvider"
            ).addClasses(
                  "org.wildfly.security.sasl.plain.PlainSaslClientFactory",
                  "org.wildfly.security.sasl.digest.DigestClientFactory",
                  "org.wildfly.security.sasl.external.ExternalSaslClientFactory",
                  "org.wildfly.security.sasl.oauth2.OAuth2SaslClientFactory",
                  "org.wildfly.security.sasl.scram.ScramSaslClientFactory",
                  "org.wildfly.security.sasl.gssapi.GssapiClientFactory",
                  "org.wildfly.security.sasl.gs2.Gs2SaslClientFactory",

                  "org.wildfly.security.password.impl.PasswordFactorySpiImpl",
                  "org.wildfly.security.password.impl.IteratedSaltedPasswordAlgorithmParametersSpiImpl",

                  // Elytron _$logger classes are spread across many jars,
                  // so Jandex cannot discover them from a single index entry.
                  "org.wildfly.security.asn1.ElytronMessages_$logger",
                  "org.wildfly.security.auth.server._private.ElytronMessages_$logger",
                  "org.wildfly.security.credential._private.ElytronMessages_$logger",
                  "org.wildfly.security.http.ElytronMessages_$logger",
                  "org.wildfly.security.keystore.ElytronMessages_$logger",
                  "org.wildfly.security.mechanism._private.ElytronMessages_$logger",
                  "org.wildfly.security.mechanism.gssapi.ElytronMessages_$logger",
                  "org.wildfly.security.password.impl.ElytronMessages_$logger",
                  "org.wildfly.security.permission.ElytronMessages_$logger",
                  "org.wildfly.security.permission.SecurityMessages_$logger",
                  "org.wildfly.security.provider.util._private.ElytronMessages_$logger",
                  "org.wildfly.security.sasl._private.ElytronMessages_$logger",
                  "org.wildfly.security.ssl.ElytronMessages_$logger",
                  "org.wildfly.security.util.ElytronMessages_$logger",
                  "org.wildfly.security.x500._private.ElytronMessages_$logger",
                  "org.wildfly.security.x500.cert._private.ElytronMessages_$logger",
                  "org.wildfly.security.x500.cert.util.ElytronMessages_$logger",

                  "org.infinispan.client.hotrod.event.impl.ContinuousQueryImpl$ClientEntryListener",
                  "org.infinispan.client.hotrod.near.NearCacheService$InvalidatedNearCacheListener",

                  "java.time.ZoneRegion"
            );
   }
}
