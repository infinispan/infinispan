package org.infinispan.persistence;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Tests the cache loader when used in a repeatable read isolation level
 *
 * @author William Burns
 * @since 6.0
 */
@Test(groups = "functional", testName = "persistence.CacheLoaderRepeatableReadFunctionalTest")
public class CacheLoaderRepeatableReadFunctionalTest extends CacheLoaderFunctionalTest {
   @Override
   protected void configure(ConfigurationBuilder cb) {
      cb.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
   }
}
