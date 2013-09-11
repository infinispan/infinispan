package org.infinispan.client.hotrod.query;

import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * Test remote queries against Infinispan Directory provider.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.query.HotRodQueryIspnDirectoryTest", groups = "functional")
@CleanupAfterMethod
public class HotRodQueryIspnDirectoryTest extends HotRodQueryTest {

   protected String getLuceneDirectoryProvider() {
      return "infinispan";
   }
}
