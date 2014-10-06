package org.infinispan.server.test.client.hotrod.security;

import org.infinispan.server.test.category.Security;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
@Category({ Security.class })
public class HotRodKrbAuthLdapAuthzIT extends HotRodKrbAuthIT {

   private static final String ARQ_CONTAINER_ID = "hotrodAuthzLdap";

   @ArquillianResource
   public ContainerController controller;

   @Before
   public void startIspnServer() {
      controller.start(ARQ_CONTAINER_ID);
   }

   @After
   public void stopIspnServer() {
      controller.stop(ARQ_CONTAINER_ID);
   }

}
