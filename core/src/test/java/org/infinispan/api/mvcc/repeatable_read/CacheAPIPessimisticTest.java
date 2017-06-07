package org.infinispan.api.mvcc.repeatable_read;

import org.infinispan.api.BaseCacheAPIPessimisticTest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "api.mvcc.repeatable_read.CacheAPIPessimisticTest")
public class CacheAPIPessimisticTest extends BaseCacheAPIPessimisticTest {
   @Override
   protected IsolationLevel getIsolationLevel() {
      return IsolationLevel.REPEATABLE_READ;
   }
}
