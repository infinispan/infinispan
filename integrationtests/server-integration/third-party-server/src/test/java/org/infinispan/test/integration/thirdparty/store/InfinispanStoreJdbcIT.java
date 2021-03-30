package org.infinispan.test.integration.thirdparty.store;

import static org.infinispan.test.integration.GenericDeploymentHelper.addLibrary;
import static org.infinispan.test.integration.thirdparty.DeploymentHelper.isTomcat;

import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.infinispan.test.integration.store.AbstractInfinispanStoreJdbcIT;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.runner.RunWith;

/**
 * Test the Infinispan JDBC CacheStore AS module integration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@RunWith(Arquillian.class)
public class InfinispanStoreJdbcIT extends AbstractInfinispanStoreJdbcIT {

   @Deployment
   @TargetsContainer("server-1")
   public static Archive<?> deployment() {
      WebArchive war = DeploymentHelper.createDeployment();
      war.addClass(AbstractInfinispanStoreJdbcIT.class);
      war.addAsResource("jdbc-config.xml");
      addLibrary(war, "org.infinispan:infinispan-core");
      addLibrary(war, "org.infinispan:infinispan-cachestore-jdbc");
      addLibrary(war, "org.infinispan:infinispan-cachestore-jdbc");
      if (isTomcat()) {
         addLibrary(war, "com.h2database:h2");
      }
      return war;
   }

}
