package org.infinispan.test.integration.thirdparty.store;

import static org.infinispan.test.integration.GenericDeploymentHelper.addLibrary;
import static org.infinispan.test.integration.thirdparty.DeploymentHelper.isTomcat;

import org.infinispan.test.integration.protostream.ServerIntegrationSCI;
import org.infinispan.test.integration.store.AbstractInfinispanStoreJpaIT;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.runner.RunWith;

/**
 * Test the Infinispan JPA CacheStore AS module integration
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class InfinispanStoreJpaIT extends AbstractInfinispanStoreJpaIT {

   @Deployment
   @TargetsContainer("server-1")
   public static Archive<?> deployment() {
      WebArchive war = DeploymentHelper.createDeployment();
      war.addClass(AbstractInfinispanStoreJpaIT.class);
      war.addClasses(ServerIntegrationSCI.CLASSES);
      war.addAsResource("META-INF/ispn-persistence.xml", "META-INF/persistence.xml");
      war.addAsResource("jpa-config.xml");
      addLibrary(war, "org.infinispan:infinispan-core");
      addLibrary(war, "org.infinispan:infinispan-cachestore-jpa");
      if (isTomcat()) {
         addLibrary(war, "com.h2database:h2");
         addLibrary(war, "org.hibernate:hibernate-core");
      }
      return war;
   }
}
