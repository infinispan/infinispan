package org.infinispan.graalvm;

import static org.infinispan.commons.configuration.io.xml.XmlPullParser.END_DOCUMENT;
import static org.infinispan.commons.configuration.io.xml.XmlPullParser.END_TAG;
import static org.infinispan.commons.configuration.io.xml.XmlPullParser.FEATURE_PROCESS_NAMESPACES;
import static org.infinispan.commons.configuration.io.xml.XmlPullParser.START_TAG;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Stream;

import org.graalvm.nativeimage.hosted.Feature;
import org.infinispan.commons.configuration.io.xml.MXParser;
import org.infinispan.commons.configuration.io.xml.XmlPullParser;
import org.infinispan.commons.configuration.io.xml.XmlPullParserException;
import org.infinispan.commons.graalvm.Bundle;
import org.infinispan.commons.graalvm.ClassLoaderFeatureAccess;
import org.infinispan.commons.graalvm.Jandex;
import org.infinispan.commons.graalvm.ReflectionProcessor;
import org.infinispan.commons.graalvm.ReflectiveClass;
import org.infinispan.commons.graalvm.Resource;
import org.infinispan.distribution.ch.impl.ConsistentHashFactory;
import org.infinispan.notifications.Listener;
import org.jboss.jandex.IndexView;
import org.jgroups.Version;
import org.jgroups.conf.ClassConfigurator;

public class NativeMetadataProvider implements org.infinispan.commons.graalvm.NativeMetadataProvider {

   static final Collection<Resource> resourceFiles = Resource.of(
         "META-INF/services/org\\.infinispan\\.configuration\\.parsing\\.ConfigurationParser",
         "META-INF/services/org\\.infinispan\\.factories\\.impl\\.ModuleMetadataBuilder",
         "META-INF/infinispan-version\\.properties",
         "org/infinispan/protostream/message-wrapping\\.proto",
         "org/infinispan/protostream/types/java/common-java-types\\.proto",
         "org/infinispan/protostream/types/java/common-java-container-types\\.proto",
         "org/infinispan/protostream/types/protobuf/any\\.proto",
         "org/infinispan/protostream/types/protobuf/duration\\.proto",
         "org/infinispan/protostream/types/protobuf/empty\\.proto",
         "org/infinispan/protostream/types/protobuf/timestamp\\.proto",
         "org/infinispan/protostream/types/protobuf/wrappers\\.proto",
         "org/infinispan/commons/user\\.commons\\.proto",
         "org/infinispan/commons/global\\.commons\\.proto",
         "org/infinispan/commons/persistence\\.commons\\.proto",
         "org/infinispan/global\\.core\\.proto",
         "org/infinispan/persistence\\.core\\.proto",
         "org/infinispan/counter/api/persistence\\.counters-api\\.proto",
         "org/infinispan/configuration/default-jgroups-udp\\.xml",
         "org/infinispan/configuration/default-jgroups-tcp\\.xml",
         "org/infinispan/configuration/default-jgroups-kubernetes\\.xml",
         "org/infinispan/configuration/default-jgroups-ec2\\.xml",
         "org/infinispan/configuration/default-jgroups-google\\.xml",
         "org/infinispan/configuration/default-jgroups-azure\\.xml",
         "org/infinispan/configuration/default-jgroups-tunnel\\.xml",
         ClassConfigurator.MAGIC_NUMBER_FILE,
         ClassConfigurator.PROTOCOL_ID_FILE,
         Version.VERSION_FILE
   );

   static final Collection<Bundle> bundles = Bundle.of(
         "jg-messages",
         "java.base:sun.security.util.Resources",
         "com.sun.org.apache.xerces.internal.impl.msg.XMLMessages"
   );

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
            resourceFiles.stream()
      ).flatMap(Function.identity());
   }

   @Override
   public Stream<Bundle> bundles() {
      return bundles.stream();
   }

   private ReflectionProcessor reflectionProcessor() {
      IndexView index = Jandex.createIndex(
            com.github.benmanes.caffeine.cache.Cache.class, // Caffeine
            org.jgroups.stack.Protocol.class, // JGroups
            org.infinispan.protostream.GeneratedSchema.class, // ProtoStream
            org.infinispan.commons.CacheException.class, // Commons
            org.infinispan.AdvancedCache.class // Core
      );
      ReflectionProcessor reflection = new ReflectionProcessor(featureAccess, index);
      caffeine(reflection);
      jgroups(reflection);
      infinispan(reflection);
      protostream(reflection);
      return reflection;
   }

   private void caffeine(ReflectionProcessor processor) {
      processor.addClasses(
            "com.github.benmanes.caffeine.cache.PDMS",
            "com.github.benmanes.caffeine.cache.PSA",
            "com.github.benmanes.caffeine.cache.PSMS",
            "com.github.benmanes.caffeine.cache.PSW",
            "com.github.benmanes.caffeine.cache.PSMW",
            "com.github.benmanes.caffeine.cache.PSAMW",
            "com.github.benmanes.caffeine.cache.PSAWMW",
            "com.github.benmanes.caffeine.cache.PSWMS",
            "com.github.benmanes.caffeine.cache.PSWMW",
            "com.github.benmanes.caffeine.cache.SILMS",
            "com.github.benmanes.caffeine.cache.SSA",
            "com.github.benmanes.caffeine.cache.SSLA",
            "com.github.benmanes.caffeine.cache.SSLMS",
            "com.github.benmanes.caffeine.cache.SSLMW",
            "com.github.benmanes.caffeine.cache.SSMS",
            "com.github.benmanes.caffeine.cache.SSMSA",
            "com.github.benmanes.caffeine.cache.SSMSAW",
            "com.github.benmanes.caffeine.cache.SSMSW",
            "com.github.benmanes.caffeine.cache.SSW"
      );
   }

   private void jgroups(ReflectionProcessor processor) {
      processor
            .addClasses(
                  org.jgroups.blocks.RequestCorrelator.class,
                  org.jgroups.protocols.MsgStats.class,
                  org.jgroups.protocols.raft.AppendEntriesResponse.class,
                  org.jgroups.tests.perf.MPerf.class,
                  org.jgroups.util.Util.AddressScope.class,
                  org.jgroups.Version.class
            )
            .addClasses(true, true,
                  org.jgroups.protocols.MsgStats.class,
                  org.jgroups.protocols.raft.AppendEntriesRequest.class,
                  org.jgroups.stack.DiagnosticsHandler.class,
                  org.jgroups.util.ThreadPool.class
            ).addImplementations(false, false,
                  org.jgroups.conf.PropertyConverter.class,
                  org.jgroups.protocols.LocalTransport.class,
                  org.jgroups.stack.MessageProcessingPolicy.class
            ).addImplementations(true, false,
                  org.jgroups.protocols.Bundler.class,
                  org.jgroups.stack.Protocol.class
            );

      processor.addClasses(jgroupsClasses().toArray(new String[0]));
   }

   private Collection<String> jgroupsClasses() {
      Set<String> classes = new TreeSet<>();
      try (InputStream source = org.jgroups.Message.class.getClassLoader().getResourceAsStream(ClassConfigurator.MAGIC_NUMBER_FILE)) {
         MXParser reader = new MXParser();
         reader.setFeature(FEATURE_PROCESS_NAMESPACES, false);
         reader.setInput(source, null);
         int eventType;
         while ((eventType = reader.next()) != END_DOCUMENT) {
            if (eventType == START_TAG) {
               if (reader.getName().equals("magic-number-class-mapping")) {
                  parseJGroupsMagicNumbers(reader, classes);
               }
            }
         }
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      return classes;
   }

   private void parseJGroupsMagicNumbers(XmlPullParser reader, Set<String> classes) throws XmlPullParserException, IOException {
      int eventType;
      while ((eventType = reader.nextTag()) != END_DOCUMENT) {
         switch (eventType) {
            case END_TAG: {
               return;
            }
            case START_TAG: {
               if (reader.getName().equals("class")) {
                  String clazz = reader.getAttributeValue(null, "name");
                  classes.add(clazz);
                  reader.next();
                  continue;
               }
               throw new IOException("Unexpected content");
            }
            default: {
               throw new IOException("Unexpected content");
            }
         }
      }
      throw new IOException("Unexpected end of document");
   }

   private void infinispan(ReflectionProcessor processor) {
      processor
            .addClasses(
                  org.infinispan.CoreModuleImpl.class,
                  org.infinispan.distribution.ch.impl.HashFunctionPartitioner.class,
                  org.infinispan.distribution.ch.impl.RESPHashFunctionPartitioner.class,
                  org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated.class,
                  org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired.class,
                  org.infinispan.notifications.cachelistener.annotation.CacheEntryModified.class,
                  org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved.class,
                  org.infinispan.remoting.transport.jgroups.JGroupsTransport.class,
                  org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup.class
            )
            .addImplementations(false, false,
                  org.jboss.logging.BasicLogger.class,
                  org.infinispan.configuration.parsing.ConfigurationParser.class,
                  org.infinispan.configuration.cache.AbstractModuleConfigurationBuilder.class,
                  org.infinispan.configuration.cache.StoreConfigurationBuilder.class,
                  org.infinispan.configuration.serializing.ConfigurationSerializer.class,
                  ConsistentHashFactory.class,
                  org.infinispan.factories.impl.ModuleMetadataBuilder.class,
                  org.infinispan.persistence.spi.NonBlockingStore.class
            )
            .addImplementations(true, false,
                  org.infinispan.util.logging.events.Messages.class,
                  org.infinispan.interceptors.AsyncInterceptor.class
            )
            .addImplementation(false, true,
                  org.infinispan.configuration.cache.StoreConfiguration.class
            )
            .addClassesWithAnnotation(false, true, Listener.class)
            .addClasses("org.infinispan.remoting.transport.jgroups.JGroupsTransport$ChannelCallbacks")
            .addClasses(
                  // XML parsing
                  "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl",
                  "com.sun.org.apache.xerces.internal.jaxp.datatype.DatatypeFactoryImpl",
                  "com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl",
                  "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl",

                  // Internal List Implementations
                  "java.util.Arrays$ArrayList",
                  "java.util.ArrayList$SubList",
                  "java.util.AbstractList$RandomAccessSubList",
                  "java.util.Collections$EmptyList",
                  "java.util.Collections$SingletonList",
                  "java.util.Collections$SynchronizedRandomAccessList",
                  "java.util.Collections$UnmodifiableRandomAccessList",
                  "java.util.ImmutableCollections$ListN",
                  "java.util.ImmutableCollections$List12",

                  // Internal Map Implementations
                  "java.util.HashMap",
                  "java.util.Collections$EmptyMap",
                  "java.util.Collections$SingletonMap",
                  "java.util.ImmutableCollections$Map1",
                  "java.util.ImmutableCollections$MapN",

                  // Internal Set Implementations
                  "java.util.Collections$EmptySet",
                  "java.util.ImmutableCollections$SetN",
                  "java.util.ImmutableCollections$Set12",
                  "java.util.Collections$SingletonSet",
                  "java.util.Collections$SynchronizedSet",
                  "java.util.Collections$UnmodifiableSet"
            );
   }

   private void protostream(ReflectionProcessor processor) {
      processor.addClasses("java.time.ZoneRegion");
   }
}
