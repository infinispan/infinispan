package org.infinispan.tasks;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "tasks.GlobalKeySetTaskTest")
public class GlobalKeySetTaskTest extends BaseDistFunctionalTest<Object, String> {

   public GlobalKeySetTaskTest() {
      sync = true;
      numOwners = 1;
      INIT_CLUSTER_SIZE = 2;
   }

   public void testGlobalKeySetTask() throws Exception {
      Cache<String,String> cache = cache(0);
      for (int i = 0; i < 1000; i++) {
         cache.put("k" + i, "v" + i);
      }

      Set<String> allKeys = GlobalKeySetTask.getGlobalKeySet(cache);

      assertEquals(1000, allKeys.size());
   }

}
