package org.infinispan.test.integration.thirdparty.remote;

import static org.infinispan.test.integration.GenericDeploymentHelper.addLibrary;

import org.infinispan.test.integration.data.Person;
import org.infinispan.test.integration.remote.AbstractHotRodClientIT;
import org.infinispan.test.integration.remote.proto.PersonSchema;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
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
   @TargetsContainer("server-1")
   public static Archive<?> deployment() {
      WebArchive war = DeploymentHelper.createDeployment();
      war.addClass(AbstractHotRodClientIT.class);
      war.addClass(Person.class);
      war.addPackage(PersonSchema.class.getPackage().getName());
      war.addAsResource("org/infinispan/test/person.proto");
      addLibrary(war, "org.infinispan:infinispan-client-hotrod");
      return war;
   }
}
