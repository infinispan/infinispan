package org.infinispan.server.functional.extensions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerTasks {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   @Test
   public void testServerTaskNoParameters() {
      RemoteCache<String, String> cache = SERVERS.hotrod().create();
      Object hello = cache.execute("hello");
      assertEquals("Hello world", hello);
   }

   @Test
   public void testServerTaskWithParameters() {
      RemoteCache<String, String> cache = SERVERS.hotrod().create();
      ArrayList<String> messages = cache.execute("hello", Collections.singletonMap("greetee", new ArrayList<>(Arrays.asList("nurse", "kitty"))));
      assertEquals(2, messages.size());
      assertEquals("Hello nurse", messages.get(0));
      assertEquals("Hello kitty", messages.get(1));
   }

   @Test
   public void testDistributedServerTaskWithParameters() {
      // We must utilise the GenericJBossMarshaller due to ISPN-8814
      RemoteCache<String, String> cache = SERVERS.hotrod().withMarshaller(GenericJBossMarshaller.class).create();
      List<String> greetings = cache.execute("dist-hello", Collections.singletonMap("greetee", "my friend"));
      assertEquals(2, greetings.size());
      for(String greeting : greetings) {
         assertTrue(greeting.matches("Hello my friend .*"));
      }
   }

   @Test
   public void testIsolatedTask() {
      RemoteCache<String, String> cache = SERVERS.hotrod().create();
      Integer i = cache.execute("isolated");
      assertEquals(1, i.intValue());
      i = cache.execute("isolated");
      assertEquals(1, i.intValue());
   }

   @Test
   public void testSharedTask() {
      RemoteCache<String, String> cache = SERVERS.hotrod().create();
      Integer i = cache.execute("shared", Collections.emptyMap(), "k");
      assertEquals(1, i.intValue());
      i = cache.execute("shared", Collections.emptyMap(), "k");
      assertEquals(2, i.intValue());
   }
}
