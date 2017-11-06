package org.infinispan.client.hotrod.query;

import java.io.IOException;

import org.testng.annotations.Test;

/**
 * Force all 'bank.proto' schema fields the were explicitly defined in initial schema as 'Stored.YES' to be not stored.
 * This should not impact any test.
 *
 * @author anistor@redhat.com
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryDslConditionsNoFieldsStoredTest")
public class RemoteQueryDslConditionsNoFieldsStoredTest extends RemoteQueryDslConditionsTest {

   @Override
   protected String loadSchema() throws IOException {
      String schema = super.loadSchema();
      return schema.replace("Store.YES", "Store.NO");
   }
}
