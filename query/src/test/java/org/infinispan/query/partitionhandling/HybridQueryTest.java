package org.infinispan.query.partitionhandling;

import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * @since 9.3
 */
@Test(groups = "functional", testName = "query.partitionhandling.HybridQueryTest")
public class HybridQueryTest extends SharedIndexTest {

   @Override
   protected String getQuery() {
      return "from " + Person.class.getName() + " p where p.age >= 0 and p.nonIndexedField = 'Pe'";
   }
}
