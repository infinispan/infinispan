package org.infinispan.test.integration.thirdparty.embedded;

import org.infinispan.test.integration.GenericDeploymentHelper;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.infinispan.test.integration.embedded.AbstractInfinispanCoreIT;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.runner.RunWith;

/**
 * Test the Infinispan AS module integration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@RunWith(Arquillian.class)
public class InfinispanCoreIT extends AbstractInfinispanCoreIT {

   @Deployment
   @TargetsContainer("server-1")
   public static Archive<?> deployment() {
      WebArchive war = DeploymentHelper.createDeployment();
      war.addClass(AbstractInfinispanCoreIT.class);
      GenericDeploymentHelper.addLibrary(war, "org.infinispan:infinispan-core");
      return war;
   }
}
