package org.infinispan.server.test.query;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.Queries;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for remote queries over HotRod on a replicated cache using Infinispan directory.
 *
 * @author Adrian Nistor
 */
@Category({ Queries.class })
@RunWith(Arquillian.class)
public class RemoteQueryIspnDirIT extends RemoteQueryIT {

   @InfinispanResource("remote-query")
   protected RemoteInfinispanServer server;

   public RemoteQueryIspnDirIT() {
      super("clustered", "repltestcache");
   }

   @Override
   protected RemoteInfinispanServer getServer() {
      return server;
   }
}
