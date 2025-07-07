package org.infinispan.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.join;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.DummyServerManagement;
import org.infinispan.server.core.DummyServerStateManager;
import org.infinispan.server.core.MockProtocolServer;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.ServerStateManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TransportFlags;
import org.junit.jupiter.api.Assertions;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.BackupManagerTest")
public class RestServerLifecycleTest extends AbstractInfinispanTest {

   public void testSchemaResource() throws Throwable {
      runTest((status, client) -> {
         try (RestResponse res = join(client.schemas().types())) {
            // After both server and manager are started, it should properly handle requests.
            if (status == Status.STARTED) {
               ResponseAssertion.assertThat(res).isOk();
               Json json = Json.read(res.body());
               assertThat(json.asList()).hasSizeGreaterThanOrEqualTo(1);
            } else {
               ResponseAssertion.assertThat(res).isServiceUnavailable();
            }
         } catch (ExecutionException | InterruptedException | TimeoutException e) {
            Assertions.fail(e);
         }
      });
   }

   public void testHealthEndpointInitialization() throws Throwable {
      runTest((status, client) -> {
         ResponseAssertion.assertThat(client.server().live()).isOk();

         if (status == Status.NOT_STARTED || status == Status.CONTAINER_STARTED) {
            ResponseAssertion.assertThat(client.server().ready()).isServiceUnavailable();
            return;
         }

         ResponseAssertion.assertThat(client.server().ready()).isOk();
      });
   }

   private void runTest(BiConsumer<Status, RestClient> test) throws Throwable {
      EmbeddedCacheManager ecm = null;
      RestServer server = null;
      RestClient client = null;

      try {
         ecm = createCacheManager();
         server = createRestServer(ecm, TestResourceTracker.getCurrentTestShortName());
         client = createRestClient(server);

         test.accept(Status.NOT_STARTED, client);

         ecm.start();
         test.accept(Status.CONTAINER_STARTED, client);

         server.postStart();
         test.accept(Status.STARTED, client);
      } finally {
         if (ecm != null) ecm.stop();
         if (server != null) server.stop();
         if (client != null) client.close();
      }
   }

   private enum Status {
      NOT_STARTED,
      CONTAINER_STARTED,
      STARTED;
   }

   private EmbeddedCacheManager createCacheManager() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().clusteredDefault().cacheManagerName("default");
      gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      EmbeddedCacheManager ecm = createClusteredCacheManager(false, gcb, null, new TransportFlags());
      BasicComponentRegistry bcr = SecurityActions.getGlobalComponentRegistry(ecm).getComponent(BasicComponentRegistry.class);
      bcr.registerComponent(ServerStateManager.class, new DummyServerStateManager(), false);
      return ecm;
   }

   private RestServer createRestServer(EmbeddedCacheManager ecm, String name) {
      RestServerConfigurationBuilder rscb = new RestServerConfigurationBuilder();
      rscb.name(name).host("localhost").port(0).maxContentLength("1MB");
      RestServer server = new RestServer();
      @SuppressWarnings("rawtypes") Map<String, ProtocolServer> protocolServers = new HashMap<>();
      server.setServerManagement(new DummyServerManagement(ecm, protocolServers), false);
      server.start(rscb.build(), ecm);
      protocolServers.put("DummyProtocol", new MockProtocolServer("DummyProtocol", server.getTransport()));
      return server;
   }

   private RestClient createRestClient(RestServer server) {
      RestClientConfigurationBuilder rccb = new RestClientConfigurationBuilder();
      rccb.addServer().host(server.getHost()).port(server.getPort());
      return RestClient.forConfiguration(rccb.build());
   }
}
