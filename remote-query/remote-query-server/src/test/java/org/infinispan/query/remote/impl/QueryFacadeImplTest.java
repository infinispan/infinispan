package org.infinispan.query.remote.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import org.infinispan.server.core.QueryFacade;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.remote.impl.QueryFacadeImplTest")
public class QueryFacadeImplTest {

   /**
    * Ensure there is exactly one loadable provider for QueryFacade.
    */
   public void testProvider() {
      List<QueryFacade> implementations = new ArrayList<>();
      for (QueryFacade impl : ServiceLoader.load(QueryFacade.class)) {
         implementations.add(impl);
      }

      assertEquals(1, implementations.size());
      assertEquals(QueryFacadeImpl.class, implementations.get(0).getClass());
   }
}
