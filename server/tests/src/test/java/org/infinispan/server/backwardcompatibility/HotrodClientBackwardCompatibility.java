package org.infinispan.server.backwardcompatibility;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.server.test.core.category.Compatibility;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @since 11.0
 * https://issues.redhat.com/browse/JDG-2386
 */

@Category(Compatibility.class)
public class HotrodClientBackwardCompatibility {

   private static final int TOTAL_ENTRIES = 5;
   private static final Integer ISPN_9 = 9;

   @ClassRule
   public static InfinispanServerRule SERVERS = BackwardCompatibilityIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void getBulkTestCompatibility() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      // This is a backward compatibility test which should only run with and older client
      assumeTrue(getHotrodClientVersion() <= ISPN_9);
      RemoteCache<String, String> remoteCache = SERVER_TEST.hotrod().create();

      Map<String, String> map = new HashMap<>();
      IntStream.rangeClosed(1, TOTAL_ENTRIES).forEach(i -> map.put("key" + i, "value" + i));
      remoteCache.putAll(map);
      //Using reflection to avoid IDE complain about missing method
      Method getBulkMethod = remoteCache.getClass().getDeclaredMethod("getBulk");
      Map<String, String> bulk = (Map<String, String>) getBulkMethod.invoke(remoteCache);

      IntStream.rangeClosed(1, TOTAL_ENTRIES).forEach(i -> {
         assertTrue(bulk.containsKey("key" + i));
         assertTrue(bulk.containsValue("value" + i));
      });

      Assert.assertEquals(TOTAL_ENTRIES, bulk.size());

      Method getBulkMethodWithSize = remoteCache.getClass().getDeclaredMethod("getBulk", int.class);
      Map<String, String> bulk2 = (Map<String, String>) getBulkMethodWithSize.invoke(remoteCache, 2);
      Assert.assertEquals(2, bulk2.size());
   }

   private int getHotrodClientVersion() {
      String fullVersion = RemoteCache.class.getPackage().getImplementationVersion();
      return Integer.parseInt(fullVersion.substring(0, fullVersion.indexOf(".")));
   }
}