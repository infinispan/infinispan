package org.infinispan.query.affinity;

import java.util.Map;

import org.testng.annotations.Test;

/**
 * @since 9.0
 */
@Test(groups = "stress", testName = "query.AffinityTopologyChangeAsyncTest")
public class AffinityTopologyChangeAsyncTest extends AffinityTopologyChangeTest {

   @Override
   protected Map<String, String> getIndexingProperties() {
      Map<String, String> properties = super.getIndexingProperties();
      properties.put("default.worker.execution", "async");
      return properties;
   }

}
