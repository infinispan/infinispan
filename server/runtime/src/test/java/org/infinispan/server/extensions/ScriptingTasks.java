package org.infinispan.server.extensions;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ScriptingTasks {

   @ClassRule
   public static InfinispanServerRule SERVERS = ExtensionsIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testSimpleScript() {
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().create();
      String scriptName = SERVER_TEST.addScript(cache.getRemoteCacheManager(), "scripts/test.js");

      cache.put("keyA", "A");
      cache.put("keyB", "B");

      Map<String, Object> parameters = new HashMap<>();
      parameters.put("key", "keyC");
      parameters.put("value", "C");
      int result = cache.execute(scriptName, parameters);

      assertEquals(3, result);
   }

   @Test
   public void testStreamingScript() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.marshaller(new JavaSerializationMarshaller()).addJavaSerialWhiteList(HashMap.class.getName());

      org.infinispan.configuration.cache.ConfigurationBuilder cacheBuilder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      cacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .encoding().key().mediaType(APPLICATION_SERIALIZED_OBJECT_TYPE)
            .encoding().value().mediaType(APPLICATION_SERIALIZED_OBJECT_TYPE);

      RemoteCache<String, String> cache = SERVER_TEST.hotrod()
            .withClientConfiguration(builder)
            .withServerConfiguration(cacheBuilder)
            .create();
      String scriptName = SERVER_TEST.addScript(cache.getRemoteCacheManager(), "scripts/stream.js");
      cache.put("key1", "Lorem ipsum dolor sit amet");
      cache.put("key2", "consectetur adipiscing elit");
      cache.put("key3", "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua");

      Map<String, Long> result = cache.execute(scriptName, Collections.emptyMap());
      assertEquals(19, result.size());
   }

}
