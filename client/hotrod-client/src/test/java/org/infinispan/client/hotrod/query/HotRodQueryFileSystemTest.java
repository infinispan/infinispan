package org.infinispan.client.hotrod.query;

import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * Tests verifying the functionality of Remote queries for HotRod using FileSystem as a directory provider.
 *
 * @author Anna Manukyan
 */
@Test(testName = "client.hotrod.query.HotRodQueryFileSystemTest", groups = "functional")
@CleanupAfterMethod
public class HotRodQueryFileSystemTest extends HotRodQueryTest {

   protected String getLuceneDirectoryProvider() {
      return "filesystem";
   }

}
