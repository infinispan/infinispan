package org.infinispan.server.extensions;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerTasks {

   @ClassRule
   public static InfinispanServerRule SERVERS = ExtensionsIT.SERVERS;

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
}
