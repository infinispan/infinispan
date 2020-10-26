package org.infinispan.test.integration.thirdparty.remote;

import static org.infinispan.test.integration.thirdparty.DeploymentHelper.addLibrary;

import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.infinispan.test.integration.remote.AbstractHotRodClientIT;
import org.infinispan.test.integration.data.Person;
import org.infinispan.test.integration.util.ITestUtils;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.runner.RunWith;

/**
 * Test the Infinispan AS remote client module integration
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class HotRodClientIT extends AbstractHotRodClientIT {

   @Deployment
   public static Archive<?> deployment() {
      WebArchive war = DeploymentHelper.createDeployment();
      war.addClass(AbstractHotRodClientIT.class);
      war.addClass(Person.class);
      war.addClass(ITestUtils.class);

      addLibrary(war, "org.infinispan:infinispan-client-hotrod");

      return war;
   }
}
