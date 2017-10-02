package org.infinispan.server.test.query;

import org.infinispan.server.test.category.Queries;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Basic remote query test with off-heap data container
 *
 * @author vjuranek
 * @since 9.2
 */
@Category(Queries.class)
@RunWith(Arquillian.class)
public class RemoteQueryOffHeapIT extends RemoteQueryIT {

   public RemoteQueryOffHeapIT() {
      super("clustered", "localtestOffHeap");
   }

}
