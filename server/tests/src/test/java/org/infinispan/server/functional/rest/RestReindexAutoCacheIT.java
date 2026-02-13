package org.infinispan.server.functional.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.server.functional.hotrod.HotRodCacheQueries.ENTITY_USER;
import static org.infinispan.server.test.core.Common.sync;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteSchemasAdmin;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationProperties;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.test.Eventually;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.IndexStartupMode;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.schema.Schema;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class RestReindexAutoCacheIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVER =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .runMode(ServerRunMode.CONTAINER)
               .property(INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED, "true")
               .build();

   @Test
   public void testReindexAuto() throws IOException {
      RemoteCache<String, User> remoteCache = initialize("autoindex", IndexStorage.LOCAL_HEAP, IndexStartupMode.AUTO);

      ContainerInfinispanServerDriver serverDriver = (ContainerInfinispanServerDriver) SERVER.getServerDriver();

      RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
            .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
            .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
      RestClient rest = SERVER.rest().withClientConfiguration(restClientBuilder).get();

      String cacheName = remoteCache.getName();
      checkIndex(rest, cacheName, serverDriver);

      stopAndRestartCluster(rest, serverDriver);
      checkIndex(rest, cacheName, serverDriver);
   }

   @Test
   public void testReindexOnCorrupt() throws IOException {
      RemoteCache<String, User> remoteCache = initialize("corruptible", IndexStorage.FILESYSTEM, IndexStartupMode.REINDEX);

      ContainerInfinispanServerDriver serverDriver = (ContainerInfinispanServerDriver) SERVER.getServerDriver();
      RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
            .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
            .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
      RestClient rest = SERVER.rest().withClientConfiguration(restClientBuilder).get();

      String cacheName = remoteCache.getName();
      checkIndex(rest, cacheName, serverDriver);

      corruptIndex(serverDriver);
      stopAndRestartCluster(rest, serverDriver);

      checkIndex(rest, cacheName, serverDriver);
   }

   private RemoteCache<String, User> initialize(String name, IndexStorage indexStorage, IndexStartupMode indexStartupMode) throws IOException {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      ProtoStreamMarshaller protoStreamMarshaller = new ProtoStreamMarshaller();
      FileDescriptorSource descriptor = FileDescriptorSource.fromString(TestDomainSCI.INSTANCE.getName(), TestDomainSCI.INSTANCE.getContent());
      protoStreamMarshaller.getSerializationContext().registerProtoFiles(descriptor);
      builder.marshaller(protoStreamMarshaller);
      builder.addContextInitializer(TestDomainSCI.INSTANCE);
      RemoteCacheManager remoteCacheManager = SERVER.hotrod().withClientConfiguration(builder).createRemoteCacheManager();
      RemoteCache<String, User> remoteCache = createQueryablePersistentCache(remoteCacheManager, name, TestDomainSCI.INSTANCE, ENTITY_USER, indexStorage, indexStartupMode);
      User user = new User();
      user.setId(1);
      user.setName("Tom");
      user.setSurname("Cat");
      user.setGender(User.Gender.MALE);

      remoteCache.put("1", user);
      return remoteCache;
   }

   private static void corruptIndex(ContainerInfinispanServerDriver serverDriver) throws IOException {
      String output = serverDriver.syncFilesFromServer(0, "data/corruptible/sample_bank_account.User");
      Path index = Path.of(output).resolve("sample_bank_account.User");
      PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:segments*");
      try (var finder = Files.find(index, 1, (path, attrs) -> matcher.matches(path.getFileName()))) {
         Path segment = finder.findFirst().get();
         try (RandomAccessFile raf = new RandomAccessFile(segment.toFile(), "rw")) {
            raf.seek(0);
            raf.write(0xFF);
            raf.write(0xFF);
         }
      }
      serverDriver.syncFilesToServer(0, index.toString(), "/opt/infinispan/server/data/corruptible");
   }

   private static void checkIndex(RestClient rest, String cacheName, ContainerInfinispanServerDriver serverDriver) {
      RestResponse response = sync(rest.cache(cacheName).get("1"));
      assertThat(response.body()).contains("Tom");
      Eventually.eventually(
            "Reindexing incomplete",
            () -> {
               RestResponse stats = sync(rest.cache(cacheName).searchStats());
               return stats.body().contains("types\":{\"sample_bank_account.User\":{\"count\":1");
            },
            serverDriver.getTimeout(), 1, TimeUnit.SECONDS);
   }

   private static void stopAndRestartCluster(RestClient rest, ContainerInfinispanServerDriver serverDriver) {
      sync(rest.cluster().stop(), 1, TimeUnit.MINUTES).close();
      Eventually.eventually(
            "Cluster did not shutdown within timeout",
            () -> (!serverDriver.isRunning(0)),
            serverDriver.getTimeout(), 1, TimeUnit.SECONDS);
      serverDriver.restartCluster();
   }

   public static <K, V> RemoteCache<K, V> createQueryablePersistentCache(RemoteCacheManager remoteCacheManager, String cacheName, Schema protoschema, String entityName, IndexStorage indexStorage, IndexStartupMode indexStartupMode) {
      RemoteSchemasAdmin schemas = remoteCacheManager.administration().schemas();
      schemas.createOrUpdate(protoschema);
      assertThat(schemas.retrieveError(protoschema.getName())).isEmpty();

      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.encoding().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).stateTransfer().awaitInitialTransfer(true);
      builder
            .persistence()
            .addSoftIndexFileStore()
            .segmented(false);
      builder.statistics().enable();
      builder.indexing().enable()
            .startupMode(indexStartupMode)
            .storage(indexStorage)
            .addIndexedEntity(entityName);
      return remoteCacheManager.administration().getOrCreateCache(cacheName, builder.build());
   }
}
