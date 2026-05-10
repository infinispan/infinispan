package org.infinispan.distribution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "distribution.InvalidationNoReplicationNoTxTest")
public class InvalidationNoReplicationNoTxTest extends InvalidationNoReplicationTest {

   public InvalidationNoReplicationNoTxTest() {
      transactional = false;
   }

   public void testInvalidation() throws Exception {
      cache(1).put(k0, "v0");
      assertTrue(advancedCache(0).getDataContainer().containsKey(k0));
      assertFalse(advancedCache(1).getDataContainer().containsKey(k0));

      assertEquals("v0", cache(1).get(k0));
      assertTrue(advancedCache(0).getDataContainer().containsKey(k0));
      assertTrue(advancedCache(1).getDataContainer().containsKey(k0));

      log.info("Here is the put!");
      log.infof("Cache 0=%s cache 1=%s", address(0), address(1));
      cache(0).put(k0, "v1");

      log.info("before assertions!");
      assertNull(advancedCache(1).getDataContainer().peek(k0));
      assertEquals("v1", advancedCache(0).getDataContainer().peek(k0).getValue());
   }

}
