package org.infinispan.test.integration.thirdparty.cdi;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.integration.GenericDeploymentHelper;
import org.infinispan.test.integration.cdi.AbstractDuplicatedDomainsCdiIT;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * Tests whether {@link DefaultCacheManager} sets custom Cache name to avoid JMX
 * name collision.
 *
 * @author Sebastian Laskawiec
 */
@Ignore("ISPN-12712")
@RunWith(Arquillian.class)
public class DuplicatedDomainsCdiIT extends AbstractDuplicatedDomainsCdiIT {

   @Deployment
   @TargetsContainer("server-1")
   public static Archive<?> deployment() {
      WebArchive war = DeploymentHelper.createDeployment();
      war.addClass(AbstractDuplicatedDomainsCdiIT.class);
      GenericDeploymentHelper.addLibrary(war, "org.infinispan:infinispan-cdi-embedded");
      return war;
   }
}
