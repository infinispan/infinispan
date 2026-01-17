package org.infinispan.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.internal.InternalCacheNames.CONFIG_STATE_CACHE_NAME;
import static org.infinispan.test.TestingUtil.join;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
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
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.testing.TestResourceTracker;
import org.infinispan.topology.LocalTopologyManager;
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

   public void testCacheCreationDoesNotChanceReadiness() throws Throwable {
      EmbeddedCacheManager ecm = null;
      RestServer server = null;
      RestClient client = null;

      try {
         ecm = createCacheManager();
         server = createRestServer(ecm, TestResourceTracker.getCurrentTestShortName());
         client = createRestClient(server);

         LocalTopologyManager original = GlobalComponentRegistry.componentOf(ecm, LocalTopologyManager.class);
         LocalTopologyManager spy = spy(original);
         TestingUtil.replaceComponent(ecm, LocalTopologyManager.class, spy, true);

         // We block the cache manager initialization by blocking the start of the config cache.
         CheckPoint checkPoint = new CheckPoint();
         doAnswer(invocation -> {
            checkPoint.trigger("local-topology-enter");
            checkPoint.awaitStrict("local-topology-wait", 10, TimeUnit.SECONDS);
            return invocation.callRealMethod();
         }).when(spy).join(eq(CONFIG_STATE_CACHE_NAME), any(), any(), any());

         Future<?> start = fork(ecm::start);

         checkPoint.awaitStrict("local-topology-enter", 10, TimeUnit.SECONDS);

         // We haven't started the server and the cache manager is still initializing.
         ResponseAssertion.assertThat(client.server().live()).isOk();
         ResponseAssertion.assertThat(client.server().ready()).isServiceUnavailable();

         // The server is initiated but the cache manager is still starting, continue unavailable.
         server.postStart();
         ResponseAssertion.assertThat(client.server().live()).isOk();
         ResponseAssertion.assertThat(client.server().ready()).isServiceUnavailable();

         // Releases the cache manager start. After this point the probe is always available.
         checkPoint.trigger("local-topology-wait");
         start.get(10, TimeUnit.SECONDS);

         ResponseAssertion.assertThat(client.server().live()).isOk();
         ResponseAssertion.assertThat(client.server().ready()).isOk();

         // We create a new cache at runtime and delay its start.
         // The probe should still continue to reply as available.
         final EmbeddedCacheManager finalCacheManager = ecm;
         Configuration configuration = new ConfigurationBuilder()
               .clustering().cacheMode(CacheMode.DIST_SYNC).build();
         doAnswer(invocation -> {
            checkPoint.trigger("local-topology-enter");
            checkPoint.awaitStrict("local-topology-wait", 10, TimeUnit.SECONDS);
            return invocation.callRealMethod();
         }).when(spy).join(eq("second-cache"), any(), any(), any());
         Future<?> cache = fork(() -> finalCacheManager.createCache("second-cache", configuration));

         checkPoint.awaitStrict("local-topology-enter", 10, TimeUnit.SECONDS);

         ResponseAssertion.assertThat(client.server().live()).isOk();
         ResponseAssertion.assertThat(client.server().ready()).isOk();

         checkPoint.trigger("local-topology-wait");
         cache.get(10, TimeUnit.SECONDS);

         ResponseAssertion.assertThat(client.server().live()).isOk();
         ResponseAssertion.assertThat(client.server().ready()).isOk();
      } finally {
         if (ecm != null) ecm.stop();
         if (server != null) server.stop();
         if (client != null) client.close();
      }
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
