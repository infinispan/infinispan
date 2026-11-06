package org.infinispan.client.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.commons.internal.InternalCacheNames.SCRIPT_CACHE_NAME;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Collections;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.bank.User;
import org.testng.annotations.Test;

/**
 * Test for scripts with application/json data type interacting with protobuf caches.
 *
 * @since 9.4
 */
@Test(groups = "functional", testName = "client.hotrod.ProtobufJsonScriptTest")
public class ProtobufJsonScriptTest extends MultiHotRodServersTest {

   private static final String SCRIPT_NAME = "protobuf-json-script.js";
   private static final int CLUSTER_SIZE = 2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      cfgBuilder.encoding().key().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      cfgBuilder.encoding().value().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      createHotRodServers(CLUSTER_SIZE, cfgBuilder);
      waitForClusterToForm();
      waitForClusterToForm(SCRIPT_CACHE_NAME);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestDomainSCI.INSTANCE;
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      return super.createHotRodClientConfigurationBuilder(host, serverPort).socketTimeout(10_000);
   }

   @Test
   public void testDataAsJSONFromScript() throws IOException {
      RemoteCacheManager remoteCacheManager = client(0);
      RemoteCache<String, User> cache = remoteCacheManager.getCache();
      cache.putAll(User.data());

      Query<User> q = cache.query("FROM sample_domain.User WHERE name = 'Jane'");
      User user = q.execute().list().iterator().next();
      assertEquals("Jane", user.getName());

      registerScript(remoteCacheManager, SCRIPT_NAME);

      // The script will clone an existing user, change some fields and insert into a new user
      User result = cache.execute(SCRIPT_NAME, Collections.emptyMap());

      // Read the user as pojo
      assertEquals(10, result.getId());
      assertEquals("Rex", result.getName());
      assertEquals(67, (int) result.getAge());
   }

   private void registerScript(RemoteCacheManager remoteCacheManager, String script) throws IOException {
      RemoteCache<String, String> scriptCache = remoteCacheManager.getCache(SCRIPT_CACHE_NAME);
      String string = Util.getResourceAsString("/" + script, getClass().getClassLoader());
      scriptCache.put(SCRIPT_NAME, string);
   }
}
