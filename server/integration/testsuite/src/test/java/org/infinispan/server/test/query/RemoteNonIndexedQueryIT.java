package org.infinispan.server.test.query;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.Queries;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for remote queries over HotRod on a local non-indexed cache.
 *
 * @author Adrian Nistor
 * @author Martin Gencur
 * @since 7.0
 */
@Category(Queries.class)
@RunWith(Arquillian.class)
public class RemoteNonIndexedQueryIT extends RemoteQueryIT {

   @InfinispanResource("remote-query-1")
   protected RemoteInfinispanServer server;

   public RemoteNonIndexedQueryIT() {
      super("clustered", "localnotindexed");
   }

   @Override
   protected RemoteInfinispanServer getServer() {
      return server;
   }

   @Test
   @Override
   public void testWayTooManyInClauses() {
      // this test is not expected to throw an exception as unindexed queries
      // do not have a limit on the number of IN clauses
   }
}
