package org.infinispan.query.partition;

import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * @since 9.3
 */
@Test(groups = "functional", testName = "query.partitionhandling.HybridQueryTest")
public class HybridQueryTest extends SharedIndexTest {

   @Override
   protected String getQuery() {
      return "From " + Person.class.getName() + " p where p.age >= 0 and p.nonIndexedField = 'Pe'";
   }
}
