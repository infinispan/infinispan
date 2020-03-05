package org.infinispan.rest.search;

import org.infinispan.query.dsl.IndexedQueryMode;
import org.testng.annotations.Test;

/**
 * Test for indexed search over Rest when using a non-shared index without specifying the query mode
 *
 * @since 11.0
 */
@Test(groups = "functional", testName = "rest.NonSharedIndexSearchAutoIndexModeTest")
public class NonSharedIndexSearchAutoIndexModeTest extends NonSharedIndexSearchTest {

   @Override
   IndexedQueryMode getQueryMode() {
      return null;
   }
}
