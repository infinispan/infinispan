package org.infinispan.test.integration.thirdparty.remote;

import static org.infinispan.test.integration.GenericDeploymentHelper.addLibrary;

import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.infinispan.test.integration.remote.AbstractMemcacheClientIT;
import org.infinispan.test.integration.memcached.SimpleMemcachedClient;
import org.infinispan.test.integration.util.ITestUtils;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
public class MemcachedClientIT extends AbstractMemcacheClientIT {

   @Deployment
   @TargetsContainer("server-1")
   public static Archive<?> deployment() {
      WebArchive war = DeploymentHelper.createDeployment();
      war.addClass(AbstractMemcacheClientIT.class);
      war.addClass(SimpleMemcachedClient.class);
      war.addClass(ITestUtils.class);
      war.addClass(org.infinispan.Version.class);
      addLibrary(war, "net.spy:spymemcached");
      addLibrary(war, "org.infinispan:infinispan-commons");
      return war;
   }
}
