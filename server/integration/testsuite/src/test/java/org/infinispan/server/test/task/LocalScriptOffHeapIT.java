package org.infinispan.server.test.task;

import static org.infinispan.test.TestingUtil.loadFileAsString;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Runs JS script on off-heap cache.
 *
 * @author vjuranek
 * @since 9.2
 */
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "off-heap")})
public class LocalScriptOffHeapIT {

   @InfinispanResource("off-heap")
   RemoteInfinispanServer server1;

   private static RemoteCacheManager remoteCacheManager;

   @Before
   public void setUp() {
      if (remoteCacheManager == null) {
         remoteCacheManager = ITestUtils.createCacheManager(server1);
      }
   }

   @Test
   public void testSimpleScript() throws IOException {
      addScripts("test.js");
      RemoteCache<String, String> rc = remoteCacheManager.getCache("off-heap-default");
      rc.clear();
      rc.put("keyA", "A");
      rc.put("keyB", "B");

      Map<String, Object> parameters = new HashMap<>();
      parameters.put("key", "keyC");
      parameters.put("value", "C");
      int result = rc.execute("test.js", parameters);

      assertEquals(3, result);
   }

   @Ignore //ISPN-8350
   @Test
   public void testStreamingScript() throws IOException {
      addScripts("stream.js");
      RemoteCache<String, String> rc = remoteCacheManager.getCache("off-heap-default");
      rc.clear();
      rc.put("key1", "Lorem ipsum dolor sit amet");
      rc.put("key2", "consectetur adipiscing elit");
      rc.put("key3", "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua");

      Map<String, Long> result = rc.execute("stream.js", Collections.emptyMap());
      assertEquals(19, result.size());
   }

   private void addScripts(String script) throws IOException {
      RemoteCache<String, String> scriptCache = remoteCacheManager.getCache("___script_cache");
      try (InputStream in = this.getClass().getClassLoader().getResourceAsStream(script)) {
         scriptCache.put(script, loadFileAsString(in));
      }
   }

}
