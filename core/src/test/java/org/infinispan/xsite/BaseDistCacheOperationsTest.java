package org.infinispan.xsite;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.distribution.MagicKey;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite")
public abstract class BaseDistCacheOperationsTest extends BaseCacheOperationsTest {

   public void testPutWithLocality() {
      MagicKey remoteOwnedKey = new MagicKey(cache("LON", 1));
      cache("LON", 0).put(remoteOwnedKey, "v_LON");
      assertEquals(cache("NYC", "lonBackup", 0).get(remoteOwnedKey), "v_LON");
      assertEquals(cache("NYC", "lonBackup", 1).get(remoteOwnedKey), "v_LON");

      MagicKey localOwnedKey = new MagicKey(cache("LON", 0));
      cache("LON", 0).put(localOwnedKey, "v_LON");
      assertEquals(cache("NYC", "lonBackup", 0).get(remoteOwnedKey), "v_LON");
      assertEquals(cache("NYC", "lonBackup", 1).get(remoteOwnedKey), "v_LON");
   }
}
