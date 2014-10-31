package org.infinispan.server.test.client.hotrod.security;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.Security;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
@Category({ Security.class })
@WithRunningServer({@RunningServer(name="hotrodAuthzLdap")})
public class HotRodKrbAuthLdapAuthzIT extends HotRodKrbAuthIT {

   @InfinispanResource("hotrodAuthzLdap")
   RemoteInfinispanServer server;
   
   @Override
   public RemoteInfinispanServer getRemoteServer() {
      return server;
   }

}
