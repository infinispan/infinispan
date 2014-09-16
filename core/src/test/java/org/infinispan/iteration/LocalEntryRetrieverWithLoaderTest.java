package org.infinispan.iteration;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Test to verify local entry behavior when a loader is present
 *
 * @author afield
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.LocalEntryRetrieverWithLoaderTest")
public class LocalEntryRetrieverWithLoaderTest extends BaseEntryRetrieverWithLoaderTest {

   public LocalEntryRetrieverWithLoaderTest() {
      super(false, CacheMode.LOCAL, "LocalEntryRetrieverWithLoaderTest");
   }
}
