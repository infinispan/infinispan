package org.infinispan.server.functional.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.server.test.core.Common.sync;

import java.io.IOException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteSchemasAdmin;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.bank.User;
import org.infinispan.protostream.types.java.CommonTypesSchema;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.jupiter.InfinispanServerExtension;
import org.infinispan.server.test.jupiter.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Smoke test that UPDATE BY QUERY with value expressions works over the REST protocol.
 *
 * @since 16.4
 */
public class RestUpdateByQueryExpressionIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVER =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .runMode(ServerRunMode.CONTAINER)
               .build();

   @Test
   public void testRestUpdateByQueryWithStringConcatenation() throws IOException {
      RemoteCache<String, User> remoteCache = initialize("expr-string-update");
      remoteCache.put("1", createUser("Tom", "Cat", 1));
      remoteCache.put("2", createUser("Adrian", "Nistor", 2));

      RestClient rest = SERVER.rest().get();

      sync(rest.cache("expr-string-update").updateByQuery(
            "UPDATE FROM " + User.ENTITY_NAME + " SET name = concat(name, '!') WHERE name = 'Tom'", false));

      assertThat(remoteCache.get("1").getName()).isEqualTo("Tom!");
      assertThat(remoteCache.get("2").getName()).isEqualTo("Adrian");
   }

   @Test
   public void testRestUpdateByQueryWithArithmeticExpression() throws IOException {
      RemoteCache<String, User> remoteCache = initialize("expr-arithmetic-update");
      remoteCache.put("1", createUser("Tom", "Cat", 1));
      remoteCache.put("2", createUser("Adrian", "Nistor", 2));

      RestClient rest = SERVER.rest().get();

      sync(rest.cache("expr-arithmetic-update").updateByQuery(
            "UPDATE FROM " + User.ENTITY_NAME + " SET id = id + 10 WHERE id = 1", false));

      assertThat(remoteCache.get("1").getId()).isEqualTo(11);
      assertThat(remoteCache.get("2").getId()).isEqualTo(2);
   }

   private RemoteCache<String, User> initialize(String cacheName) throws IOException {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      ProtoStreamMarshaller protoStreamMarshaller = new ProtoStreamMarshaller();
      protoStreamMarshaller.register(new CommonTypesSchema());
      FileDescriptorSource descriptor = FileDescriptorSource.fromString(TestDomainSCI.INSTANCE.getName(), TestDomainSCI.INSTANCE.getContent());
      protoStreamMarshaller.getSerializationContext().registerProtoFiles(descriptor);
      builder.marshaller(protoStreamMarshaller);
      builder.addContextInitializer(TestDomainSCI.INSTANCE);
      RemoteCacheManager remoteCacheManager = SERVER.hotrod().withClientConfiguration(builder).createRemoteCacheManager();

      RemoteSchemasAdmin schemas = remoteCacheManager.administration().schemas();
      schemas.createOrUpdate(TestDomainSCI.INSTANCE);
      assertThat(schemas.retrieveError(TestDomainSCI.INSTANCE.getName())).isEmpty();

      org.infinispan.configuration.cache.ConfigurationBuilder cacheBuilder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      cacheBuilder.encoding().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      cacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC).stateTransfer().awaitInitialTransfer(true);
      cacheBuilder.indexing().enable().storage(IndexStorage.LOCAL_HEAP).addIndexedEntity(User.ENTITY_NAME);
      return remoteCacheManager.administration().getOrCreateCache(cacheName, cacheBuilder.build());
   }

   private User createUser(String name, String surname, int id) {
      User user = new User();
      user.setId(id);
      user.setName(name);
      user.setSurname(surname);
      return user;
   }
}
