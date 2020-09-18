package org.infinispan.test.integration.thirdparty;

import org.infinispan.test.integration.DeploymentHelper;
import org.infinispan.test.integration.commons.client.AbstractMemcacheClientIT;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
public class MemcachedClientIT extends AbstractMemcacheClientIT {

   @Deployment
   public static Archive<?> deployment() {
      WebArchive war = DeploymentHelper.createDeployment();
      war.addClass(AbstractMemcacheClientIT.class);
      return war;
   }
}
