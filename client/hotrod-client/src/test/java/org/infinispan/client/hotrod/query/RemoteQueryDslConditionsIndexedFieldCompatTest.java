package org.infinispan.client.hotrod.query;

import java.io.IOException;

import org.testng.annotations.Test;

//TODO [anistor] To be removed in Infinispan 10

/**
 * Test behaviour of the (deprecated) {code @IndexedField} annotation. It should still work without causing regressions,
 * until it finally gets removed by Infinispan 10.
 *
 * @author anistor@redhat.com
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryDslConditionsIndexedFieldCompatTest")
public class RemoteQueryDslConditionsIndexedFieldCompatTest extends RemoteQueryDslConditionsTest {

   @Override
   protected String loadSchema() throws IOException {
      String schema = super.loadSchema();
      return schema.replace("@Field(store = Store.NO, indexNullAs = \"-1\")", "@IndexedField(store=false)").replace("@SortableField", "");
   }
}
