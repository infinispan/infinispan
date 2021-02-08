package org.infinispan.test.integration.thirdparty.cdi;

import org.infinispan.test.integration.cdi.AbstractGreetingCacheManagerIT;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * @author Kevin Pollet &lt;pollet.kevin@gmail.com&gt; (C) 2011
 */
@Ignore("ISPN-12713")
@RunWith(Arquillian.class)
public class GreetingCacheManagerIT extends AbstractGreetingCacheManagerIT {

   @Deployment
   public static Archive<?> deployment() {
      WebArchive war = DeploymentHelper.createDeployment();
      war.addClass(AbstractGreetingCacheManagerIT.class);
      war.addAsWebInfResource("beans.xml");
      DeploymentHelper.addLibrary(war, "org.infinispan:infinispan-cdi-embedded");
      DeploymentHelper.addLibrary(war, "org.infinispan:infinispan-jcache");
      return war;
   }
}
