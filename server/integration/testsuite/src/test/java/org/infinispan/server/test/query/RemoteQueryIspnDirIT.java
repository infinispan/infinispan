package org.infinispan.server.test.query;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.runner.RunWith;

/**
 * Tests for remote queries over HotRod on a replicated cache using Infinispan directory.
 *
 * @author Adrian Nistor
 */
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "remote-query-infinispan-dir")})
public class RemoteQueryIspnDirIT extends RemoteQueryIT {

   @InfinispanResource("remote-query-infinispan-dir")
   protected RemoteInfinispanServer server;

   public RemoteQueryIspnDirIT() {
      super("clustered", "testcache");
   }

   @Override
   protected RemoteInfinispanServer getServer() {
      return server;
   }
}
