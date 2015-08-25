package org.infinispan.functional.expiry;

import org.infinispan.Cache;
import org.infinispan.expiry.ExpiryTest;
import org.infinispan.functional.decorators.FunctionalAdvancedCache;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "functional.expiry.FunctionalExpiryTest")
public class FunctionalExpiryTest extends ExpiryTest {

   @Override
   protected Cache<String, String> getCache() {
      Cache<String, String> cache = super.getCache();
      return FunctionalAdvancedCache.create(cache.getAdvancedCache());
   }

}
