package org.infinispan.server.extensions;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ScriptingTasks {

   @ClassRule
   public static final InfinispanServerRule SERVERS = ExtensionsIT.SERVERS;

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
      builder.addJavaSerialAllowList(HashMap.class.getName());

      org.infinispan.configuration.cache.ConfigurationBuilder cacheBuilder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      cacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .encoding().key().mediaType(APPLICATION_SERIALIZED_OBJECT_TYPE)
            .encoding().value().mediaType(APPLICATION_SERIALIZED_OBJECT_TYPE);

      RemoteCache<String, String> cache = SERVER_TEST.hotrod()
            .withClientConfiguration(builder)
            .withMarshaller(JavaSerializationMarshaller.class)
            .withServerConfiguration(cacheBuilder)
            .create();
      String scriptName = SERVER_TEST.addScript(cache.getRemoteCacheManager(), "scripts/stream.js");
      cache.put("key1", "Lorem ipsum dolor sit amet");
      cache.put("key2", "consectetur adipiscing elit");
      cache.put("key3", "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua");

      Map<String, Long> result = cache.execute(scriptName, Collections.emptyMap());
      assertEquals(19, result.size());
   }

   @Test
   public void testProtoStreamMarshaller() {
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().withMarshaller(ProtoStreamMarshaller.class).create();
      List<String> greetings = cache.execute("dist-hello", Collections.singletonMap("greetee", "my friend"));
      assertEquals(2, greetings.size());
      for(String greeting : greetings) {
         assertTrue(greeting.matches("Hello my friend .*"));
      }
   }
}
