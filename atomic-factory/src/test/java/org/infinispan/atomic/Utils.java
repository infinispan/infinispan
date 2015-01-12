package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.test.TestingUtil;

import java.util.Collection;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Pierre Sutra
 * @since 7.2
 */
public class Utils {

   public static void assertOnAllCaches(Collection<Cache> caches, Object key, String value) {
      for (Cache<Object, String> c : caches) {
         Object realVal = c.get(key);
         if (value == null) {
            assertEquals("Expecting [" + key + "] to equal [" + value + "] on cache "+ c.toString(), null, realVal);
         } else {
            assert value.equals(realVal) : "Expecting [" + key + "] to equal [" + value + "] on cache "+c.toString();
         }
      }
      // Allow some time for all ClusteredGetCommands to finish executing
      TestingUtil.sleepThread(1000);
   }
   
}
