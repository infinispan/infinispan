package org.infinispan.server.test.query;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.Queries;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for remote queries over HotRod on a local non-indexed cache.
 *
 * @author Adrian Nistor
 * @author Martin Gencur
 * @since 7.0
 */
@Category({ Queries.class })
@RunWith(Arquillian.class)
public class RemoteNonIndexedQueryIT extends RemoteQueryIT {

   @InfinispanResource("remote-query")
   protected RemoteInfinispanServer server;

   public RemoteNonIndexedQueryIT() {
      super("clustered", "localnotindexed");
   }

   @Override
   protected RemoteInfinispanServer getServer() {
      return server;
   }
}
