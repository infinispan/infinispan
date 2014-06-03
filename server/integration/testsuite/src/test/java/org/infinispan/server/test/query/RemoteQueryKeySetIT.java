package org.infinispan.server.test.query;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.protostream.sampledomain.User;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Set;

import static org.junit.Assert.*;

/**
 * Tests for keySet() method on a distributed remote cache that uses protobuf marshalling.
 *
 * @author anistor@redhat.com
 */
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "remote-query-keySet")})
public class RemoteQueryKeySetIT extends RemoteQueryBaseIT {

   @InfinispanResource("remote-query-keySet")
   protected RemoteInfinispanServer server;

   public RemoteQueryKeySetIT() {
      super("clustered", "testcache");
   }

   @Override
   protected RemoteInfinispanServer getServer() {
      return server;
   }

   @Test
   public void testDistributedKeySet() throws Exception {
      remoteCache.put(1, createUser(1));
      remoteCache.put(2, createUser(2));

      Set<Integer> keys = remoteCache.keySet();
      assertNotNull(keys);
      assertEquals(2, keys.size());
      assertTrue(keys.contains(1));
      assertTrue(keys.contains(2));
   }

   private User createUser(int id) {
      User user = new User();
      user.setId(id);
      user.setName("John " + id);
      user.setSurname("Doe");
      user.setGender(User.Gender.MALE);
      return user;
   }
}
