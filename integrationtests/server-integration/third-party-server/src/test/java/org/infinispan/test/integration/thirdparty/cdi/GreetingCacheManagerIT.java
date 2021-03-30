package org.infinispan.test.integration.thirdparty.cdi;

import org.infinispan.test.integration.GenericDeploymentHelper;
import org.infinispan.test.integration.cdi.AbstractGreetingCacheManagerIT;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.runner.RunWith;

/**
 * @author Kevin Pollet &lt;pollet.kevin@gmail.com&gt; (C) 2011
 */
@RunWith(Arquillian.class)
public class GreetingCacheManagerIT extends AbstractGreetingCacheManagerIT {

   @Deployment
   @TargetsContainer("server-1")
   public static Archive<?> deployment() {
      WebArchive war = DeploymentHelper.createDeployment();
      war.addPackage(AbstractGreetingCacheManagerIT.class.getPackage());
      war.addAsWebInfResource("beans.xml");
      GenericDeploymentHelper.addLibrary(war, "org.infinispan:infinispan-cdi-embedded");
      GenericDeploymentHelper.addLibrary(war, "org.infinispan:infinispan-jcache");
      // The JCache dependency is in the provided scope, so it's not added automatically
      GenericDeploymentHelper.addLibrary(war, "javax.cache:cache-api");
      return war;
   }
}
