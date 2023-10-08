package org.infinispan.rest.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;

/**
 * @since 12.1
 */
public abstract class MultiNodeRestTest extends MultipleCacheManagersTest {
   private RestClient client;
   protected Map<String, RestCacheClient> cacheClients;
   private final List<RestServerHelper> restServers = new ArrayList<>();

   abstract int getMembers();

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalBuilder.serialization().addContextInitializer(new org.infinispan.query.remote.impl.persistence.PersistenceContextInitializerImpl());
      createClusteredCaches(getMembers(), globalBuilder, new ConfigurationBuilder(), true);

      cacheManagers.forEach(cm -> {
         RestServerHelper restServer = new RestServerHelper(cm);
         restServer.start(TestResourceTracker.getCurrentTestShortName());
         restServers.add(restServer);
      });

      RestClientConfigurationBuilder clientConfigurationBuilder = new RestClientConfigurationBuilder();
      restServers.forEach(s -> clientConfigurationBuilder.addServer().host(s.getHost()).port(s.getPort()));

      this.client = RestClient.forConfiguration(clientConfigurationBuilder.build());

      // Register the proto schema before starting the caches
      String protoFileContents = Util.getResourceAsString(getProtoFile(), getClass().getClassLoader());
      registerProtobuf(protoFileContents);

      cacheManagers.forEach(cm -> {
         getCacheConfigs().forEach((name, configBuilder) -> cm.createCache(name, configBuilder.build()));
      });
      cacheClients = getCacheConfigs().keySet().stream().collect(Collectors.toMap(Function.identity(), client::cache));
   }

   @AfterClass(alwaysRun = true)
   public void tearDown() throws Exception {
      client.close();
      restServers.forEach(RestServerHelper::stop);
   }

   @AfterMethod
   @Override
   protected void clearContent() {
   }

   protected void registerProtobuf(String protoFileContents) {
      CompletionStage<RestResponse> response = client.schemas().post("file.proto", protoFileContents);
      ResponseAssertion.assertThat(response).hasNoErrors();
   }

   protected abstract Map<String, ConfigurationBuilder> getCacheConfigs();

   protected abstract String getProtoFile();
}
