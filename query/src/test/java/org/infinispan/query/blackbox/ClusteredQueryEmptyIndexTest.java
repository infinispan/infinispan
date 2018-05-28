package org.infinispan.query.blackbox;

import java.util.stream.IntStream;

import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * Tests for clustered queries where some of the local indexes are empty
 * @since 9.1
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredQueryEmptyIndexTest")
public class ClusteredQueryEmptyIndexTest extends ClusteredQueryTest {

   protected void prepareTestData() {
      IntStream.range(0, NUM_ENTRIES).boxed()
            .map(i -> new Person("name" + i, "blurb" + i, i)).forEach(p -> cacheAMachine1.put(p.getName(), p));

      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

}
