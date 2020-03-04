package org.infinispan.server.extensions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerTasks {

   @ClassRule
   public static final InfinispanServerRule SERVERS = ExtensionsIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testServerTaskNoParameters() {
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().create();
      Object hello = cache.execute("hello", Collections.emptyMap());
      assertEquals("Hello world", hello);
   }

   @Test
   public void testServerTaskWithParameters() {
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().create();
      String hello = cache.execute("hello", Collections.singletonMap("greetee", "my friend"));
      assertEquals("Hello my friend", hello);
   }

   @Test
   public void testDistributedServerTaskWithParameters() {
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().create();
      List<String> greetings = cache.execute("dist-hello", Collections.singletonMap("greetee", "my friend"));
      assertEquals(2, greetings.size());
      for(String greeting : greetings) {
         assertTrue(greeting.matches("Hello my friend .*"));
      }
   }
}
