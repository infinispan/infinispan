package org.infinispan.server.extensions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
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
      Object hello = cache.execute("hello");
      assertEquals("Hello world", hello);
   }

   @Test
   public void testServerTaskWithParameters() {
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().create();
      ArrayList<String> messages = cache.execute("hello", Collections.singletonMap("greetee", new ArrayList<>(Arrays.asList("nurse", "kitty"))));
      assertEquals(2, messages.size());
      assertEquals("Hello nurse", messages.get(0));
      assertEquals("Hello kitty", messages.get(1));
   }

   @Test
   public void testDistributedServerTaskWithParameters() {
      // We must utilise the GenericJBossMarshaller due to ISPN-8814
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().withMarshaller(GenericJBossMarshaller.class).create();
      List<String> greetings = cache.execute("dist-hello", Collections.singletonMap("greetee", "my friend"));
      assertEquals(2, greetings.size());
      for(String greeting : greetings) {
         assertTrue(greeting.matches("Hello my friend .*"));
      }
   }

   @Test
   public void testIsolatedTask() {
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().create();
      Integer i = cache.execute("isolated");
      assertEquals(1, i.intValue());
      i = cache.execute("isolated");
      assertEquals(1, i.intValue());
   }

   @Test
   public void testSharedTask() {
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().create();
      Integer i = cache.execute("shared", Collections.emptyMap(), "k");
      assertEquals(1, i.intValue());
      i = cache.execute("shared", Collections.emptyMap(), "k");
      assertEquals(2, i.intValue());
   }
}
