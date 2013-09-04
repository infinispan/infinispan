package org.infinispan.query.remote;

import org.infinispan.server.core.QueryFacade;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import static org.junit.Assert.assertEquals;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.remote.QueryFacadeImplTest")
public class QueryFacadeImplTest {

   /**
    * Test there is exactly one loadable provider.
    */
   public void testProvider() {
      List<QueryFacade> implementations = new ArrayList<QueryFacade>();
      for (QueryFacade impl : ServiceLoader.load(QueryFacade.class)) {
         implementations.add(impl);
      }

      assertEquals(1, implementations.size());
      assertEquals(QueryFacadeImpl.class, implementations.get(0).getClass());
   }
}
