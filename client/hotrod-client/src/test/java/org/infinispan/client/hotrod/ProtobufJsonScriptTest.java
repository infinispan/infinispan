package org.infinispan.client.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.commons.util.Util.getResourceAsString;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.scripting.ScriptingManager.SCRIPT_CACHE;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.io.IOException;
import java.util.Collections;

import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.testng.annotations.Test;

/**
 * Test for scripts with application/json data type interacting with protobuf caches.
 *
 * @since 9.4
 */
@Test(groups = "functional", testName = "client.hotrod.ProtobufJsonScriptTest")
public class ProtobufJsonScriptTest extends MultiHotRodServersTest {

   private static final String PROTO_NAME = "/sample_bank_account/bank.proto";
   private static final String SCRIPT_NAME = "protobuf-json-script.js";
   private static final int CLUSTER_SIZE = 2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      cfgBuilder.encoding().key().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      cfgBuilder.encoding().value().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      createHotRodServers(CLUSTER_SIZE, cfgBuilder);
      waitForClusterToForm();

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = client(0).getCache(PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put(PROTO_NAME, getResourceAsString(PROTO_NAME, getClass().getClassLoader()));
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(client(0)));
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      return super.createHotRodClientConfigurationBuilder(host, serverPort).marshaller(new ProtoStreamMarshaller());
   }

   @Test
   public void testDataAsJSONFromScript() throws IOException {
      RemoteCacheManager remoteCacheManager = client(0);
      RemoteCache<Integer, User> cache = remoteCacheManager.getCache();

      User user1 = new UserPB();
      user1.setId(1);
      user1.setName("Tom");
      user1.setSurname("Cat");
      user1.setGender(User.Gender.MALE);
      user1.setAge(33);
      user1.setAccountIds(Collections.singleton(12));

      User user2 = new UserPB();
      user2.setId(2);
      user2.setName("Jane");
      user2.setSurname("Doe");
      user2.setGender(User.Gender.FEMALE);
      user2.setAge(39);

      cache.put(1, user1);
      cache.put(2, user2);

      Query q = Search.getQueryFactory(cache).create("From sample_bank_account.User where name = 'Jane'");
      User user = (User) q.list().iterator().next();
      assertEquals("Jane", user.getName());

      registerScript(remoteCacheManager, SCRIPT_NAME);

      // The script will clone an existing user, change some fields and insert into a new user
      User result = cache.execute(SCRIPT_NAME, Collections.emptyMap());

      // Read the user as pojo
      assertEquals(result.getId(), 3);
      assertEquals(result.getName(), "Rex");
      assertEquals((int) result.getAge(), 67);
   }

   private void registerScript(RemoteCacheManager remoteCacheManager, String script) throws IOException {
      RemoteCache<String, String> scriptCache = remoteCacheManager.getCache(SCRIPT_CACHE);
      String string = Util.getResourceAsString("/" + script, getClass().getClassLoader());
      scriptCache.put(SCRIPT_NAME, string);
   }

}
