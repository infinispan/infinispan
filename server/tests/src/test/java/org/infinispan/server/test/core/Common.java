package org.infinispan.server.test.core;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

import org.apache.logging.log4j.core.util.StringBuilderWriter;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.client.hotrod.security.BasicCallbackHandler;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.persistence.PersistenceIT;
import org.infinispan.server.test.api.HotRodTestClientDriver;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.test.TestingUtil;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.wildfly.security.http.HttpConstants;
import org.wildfly.security.sasl.util.SaslMechanismInformation;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class Common {
   private static final boolean IS_IBM = System.getProperty("java.vendor").contains("IBM");

   public static final Collection<String> SASL_MECHS = List.of(
         "",
         SaslMechanismInformation.Names.PLAIN,

         SaslMechanismInformation.Names.DIGEST_MD5,
         SaslMechanismInformation.Names.DIGEST_SHA_512,
         SaslMechanismInformation.Names.DIGEST_SHA_384,
         SaslMechanismInformation.Names.DIGEST_SHA_256,
         SaslMechanismInformation.Names.DIGEST_SHA,

         SaslMechanismInformation.Names.SCRAM_SHA_512,
         SaslMechanismInformation.Names.SCRAM_SHA_384,
         SaslMechanismInformation.Names.SCRAM_SHA_256,
         SaslMechanismInformation.Names.SCRAM_SHA_1
   );
   public static final Collection<Arguments> SASL_MECH_ARGUMENTS = SASL_MECHS.stream()
         .map(Arguments::of)
         .collect(Collectors.toList());

   public static final Collection<String> SASL_KERBEROS = List.of(
         "",
         SaslMechanismInformation.Names.GSSAPI,
         SaslMechanismInformation.Names.GS2_KRB5
   );

   public static final Collection<String> HTTP_MECHS = List.of(
         "",
         HttpConstants.BASIC_NAME,
         HttpConstants.DIGEST_NAME
   );

   public static final Collection<String> HTTP_KERBEROS_MECHS = List.of(
         "",
         HttpConstants.SPNEGO_NAME
   );

   public static final Collection<Protocol> HTTP_PROTOCOLS = Arrays.asList(Protocol.values());

   public static final String[] NASHORN_DEPS = new String[]{
         "org.openjdk.nashorn:nashorn-core:15.4",
         "org.ow2.asm:asm:9.5",
         "org.ow2.asm:asm-analysis:9.5",
         "org.ow2.asm:asm-commons:9.5",
         "org.ow2.asm:asm-tree:9.5",
         "org.ow2.asm:asm-util:9.5"
   };

   public static class SaslMechsArgumentProvider implements ArgumentsProvider {
      @Override
      public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
         return SASL_MECH_ARGUMENTS.stream();
      }
   }

   public static class DatabaseProvider implements ArgumentsProvider {
      @Override
      public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
         return Arrays.stream(PersistenceIT.DATABASE_LISTENER.getDatabaseTypes())
               .map(PersistenceIT.DATABASE_LISTENER::getDatabase)
               .map(Arguments::of);
      }
   }

   public static <T> T awaitStatus(Supplier<CompletionStage<RestResponse>> request, int pendingStatus, int completeStatus, Function<RestResponse, T> f) {
      int MAX_RETRIES = 100;
      for (int i = 0; i < MAX_RETRIES; i++) {
         try (RestResponse response = sync(request.get())) {
            if (response.getStatus() == pendingStatus) {
               TestingUtil.sleepThread(100);
            } else if (response.getStatus() == completeStatus) {
               return f.apply(response);
            } else {
               fail(String.format("Request returned unexpected status %d instead of %d or %d", response.getStatus(), pendingStatus, completeStatus));
            }
         }
      }
      fail(String.format("Request did not complete with status %d after %d retries", completeStatus, MAX_RETRIES));
      return null; // Never executed
   }

   public static void awaitStatus(Supplier<CompletionStage<RestResponse>> request, int pendingStatus, int completeStatus) {
      awaitStatus(request, pendingStatus, completeStatus, r -> null);
   }

   public static RestResponse awaitResponse(Supplier<CompletionStage<RestResponse>> request, int pendingStatus, int completeStatus) {
      int MAX_RETRIES = 100;
      for (int i = 0; i < MAX_RETRIES; i++) {
         RestResponse response = sync(request.get());
         if (response.getStatus() == pendingStatus) {
            response.close();
            TestingUtil.sleepThread(100);
         } else if (response.getStatus() == completeStatus) {
            return response;
         } else {
            response.close();
            fail(String.format("Request returned unexpected status %d instead of %d or %d", response.getStatus(), pendingStatus, completeStatus));
         }
      }
      fail(String.format("Request did not complete with status %d after %d retries", completeStatus, MAX_RETRIES));
      return null; // Never executed
   }

   public static <T> T sync(CompletionStage<T> stage) {
      return sync(stage, 10, TimeUnit.SECONDS);
   }

   public static <T> T sync(CompletionStage<T> stage, long timeout, TimeUnit timeUnit) {
      return Exceptions.unchecked(() -> stage.toCompletableFuture().get(timeout, timeUnit));
   }

   public static String assertStatus(int status, CompletionStage<RestResponse> request) {
      try (RestResponse response = sync(request)) {
         String body = response.getBody();
         assertEquals(status, response.getStatus(), body);
         return body;
      }
   }

   public static void assertStatusAndBodyEquals(int status, String body, CompletionStage<RestResponse> response) {
      assertEquals(body, assertStatus(status, response));
   }

   public static void assertStatusAndBodyContains(int status, String body, CompletionStage<RestResponse> response) {
      String responseBody = assertStatus(status, response);
      assertTrue(responseBody.contains(body), responseBody);
   }

   public static void assertResponse(int status, CompletionStage<RestResponse> request, Consumer<RestResponse> consumer) {
      try (RestResponse response = sync(request)) {
         assertEquals(status, response.getStatus());
         consumer.accept(response);
      }
   }

   public static Subject createSubject(String principal, String realm, char[] password) {
      return Exceptions.unchecked(() -> {
         LoginContext context = new LoginContext("KDC", null, new BasicCallbackHandler(principal, realm, password), createJaasConfiguration(false));
         context.login();
         return context.getSubject();
      });
   }

   private static Configuration createJaasConfiguration(boolean server) {
      return new Configuration() {
         @Override
         public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            if (!"KDC".equals(name)) {
               throw new IllegalArgumentException(String.format("Unexpected name '%s'", name));
            }

            AppConfigurationEntry[] entries = new AppConfigurationEntry[1];
            Map<String, Object> options = new HashMap<>();
            //options.put("debug", "true");
            options.put("refreshKrb5Config", "true");

            if (IS_IBM) {
               options.put("noAddress", "true");
               options.put("credsType", server ? "acceptor" : "initiator");
               entries[0] = new AppConfigurationEntry("com.ibm.security.auth.module.Krb5LoginModule", REQUIRED, options);
            } else {
               options.put("storeKey", "true");
               options.put("isInitiator", server ? "false" : "true");
               entries[0] = new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule", REQUIRED, options);
            }
            return entries;
         }

      };
   }

   public static String cacheConfigToJson(String name, org.infinispan.configuration.cache.Configuration configuration) {
      StringBuilderWriter sw = new StringBuilderWriter();
      try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(APPLICATION_JSON).prettyPrint(false).build()) {
         new ParserRegistry().serialize(w, name, configuration);
      }
      return sw.toString();
   }


   public static <K, V> RemoteCache<K, V> createQueryableCache(InfinispanServerExtension server, boolean indexed, String protoFile, String entityName) {

      ConfigurationBuilder config = new ConfigurationBuilder();
      config.marshaller(new ProtoStreamMarshaller());

      HotRodTestClientDriver hotRodTestClientDriver = server.hotrod().withClientConfiguration(config);
      RemoteCacheManager remoteCacheManager = hotRodTestClientDriver.createRemoteCacheManager();

      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      String schema = Exceptions.unchecked(() -> Util.getResourceAsString(protoFile, server.getClass().getClassLoader()));
      metadataCache.putIfAbsent(protoFile, schema);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertNotNull(metadataCache.get(protoFile));

      Exceptions.unchecked(() -> MarshallerRegistration.registerMarshallers(MarshallerUtil.getSerializationContext(remoteCacheManager)));

      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.encoding().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).stateTransfer().awaitInitialTransfer(true);
      if (indexed) {
         builder.indexing().enable()
               .storage(LOCAL_HEAP)
               .addIndexedEntity(entityName);
      }
      return hotRodTestClientDriver.withServerConfiguration(builder).create();
   }

   public static void assertAnyEquals(Object expected, Object actual) {
      if (expected instanceof byte[] && actual instanceof byte[])
         assertArrayEquals((byte[]) expected, (byte[]) actual);
      else
         assertEquals(expected, actual);
   }

   public static Integer getIntKeyForServer(RemoteCache<Integer, ?> cache, int server) {
      ChannelFactory cf = cache.getRemoteCacheManager().getChannelFactory();
      byte[] name = RemoteCacheManager.cacheNameBytes(cache.getName());
      InetSocketAddress serverAddress = cf.getServers(name).stream().skip(server).findFirst().get();
      DataFormat df = cache.getDataFormat();
      ConsistentHash ch = cf.getConsistentHash(name);
      Random r = new Random();
      for(int i = 0; i < 1000; i++) {
         int aInt = r.nextInt();
         SocketAddress keyAddress = ch.getServer(df.keyToBytes(aInt));
         if (keyAddress.equals(serverAddress)) {
            return aInt;
         }
      }
      throw new IllegalStateException("Could not find any key owned by " + serverAddress);
   }
}
