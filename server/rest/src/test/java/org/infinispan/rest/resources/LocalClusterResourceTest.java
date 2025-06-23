package org.infinispan.rest.resources;

import org.assertj.core.api.Assertions;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.TestClass;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.authentication.impl.BasicAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.rest.resources.security.SimpleSecurityDomain;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.DummyServerStateManager;
import org.infinispan.server.core.ServerStateManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.infinispan.commons.util.concurrent.CompletionStages.join;
import static org.infinispan.rest.resources.AbstractRestResourceTest.ADMIN;
import static org.infinispan.rest.resources.AbstractRestResourceTest.REALM;
import static org.infinispan.rest.resources.AbstractRestResourceTest.USER;
import static org.infinispan.rest.resources.ClusterResource.CACHE_MANAGER_STATUS;
import static org.infinispan.rest.resources.ClusterResource.MEMBERS;
import static org.infinispan.rest.resources.ClusterResource.NODE_ADDRESS;
import static org.infinispan.rest.resources.ClusterResource.PHYSICAL_ADDRESSES;
import static org.infinispan.rest.resources.ClusterResource.ROLLING_UPGRADE;
import static org.infinispan.rest.resources.ClusterResource.VERSION;

@Test(groups = "functional", testName = "rest.LocalClusterResourceTest")
public class LocalClusterResourceTest extends SingleCacheManagerTest {
   RestServerHelper restServer;
   protected RestClient adminClient;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(true);
      BasicComponentRegistry bcr = SecurityActions.getGlobalComponentRegistry(cm).getComponent(BasicComponentRegistry.class);
      ServerStateManager serverStateManager = new DummyServerStateManager();
      bcr.registerComponent(ServerStateManager.class, serverStateManager, false);
      cm.getClassAllowList().addClasses(TestClass.class);
      restServer = new RestServerHelper(cm);
      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(new SimpleSecurityDomain(ADMIN, USER), REALM);
      restServer.withAuthenticator(basicAuthenticator);
      restServer.start(TestResourceTracker.getCurrentTestShortName());
      adminClient = RestClient.forConfiguration(getClientConfig("admin", "admin").build());
      return cm;
   }

   @AfterClass(alwaysRun=true)
   protected void destroyAfterClass() {
      try {
         restServer.stop();
         super.destroyAfterClass();
      } catch (Exception e) {
         log.error("Unexpected!", e);
      }
   }

   protected RestClientConfigurationBuilder getClientConfig(String username, String password) {
      RestClientConfigurationBuilder clientConfigurationBuilder = new RestClientConfigurationBuilder();
         clientConfigurationBuilder.security().authentication().enable().username(username).password(password);
      clientConfigurationBuilder.addServer().host(restServer.getHost()).port(restServer.getPort());
      return clientConfigurationBuilder;
   }

   @Test
   public void testClusterMembership() {
      RestResponse response = join(adminClient.cluster().clusterMembers());
      String cmVersion = cacheManager.getCacheManagerInfo().getVersion();
      ResponseAssertion.assertThat(response).isOk();
      Json clusterMembership = Json.read(response.body());
      List<Json> members = clusterMembership.at(MEMBERS).asJsonList();
      Assertions.assertThat(members.size()).isOne();
      Json localMember = members.get(0);
      Assertions.assertThat(localMember.at(NODE_ADDRESS).asString()).isEqualTo("local");
      Assertions.assertThat(localMember.at(VERSION).asString()).isEqualTo(cmVersion);
      Assertions.assertThat(localMember.at(CACHE_MANAGER_STATUS).asString()).isEqualTo("RUNNING");
      Assertions.assertThat(localMember.at(PHYSICAL_ADDRESSES).asString()).isEqualTo("local");
      Assertions.assertThat(clusterMembership.at(ROLLING_UPGRADE).asBoolean()).isFalse();
   }

   @Test
   public void testConnectedClients() {
      CompletionStage<RestResponse> response = adminClient.server().listConnections(true);
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).containsReturnedText("[]");
      response = adminClient.server().listConnections(false);
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).containsReturnedText("[]");
   }
}
