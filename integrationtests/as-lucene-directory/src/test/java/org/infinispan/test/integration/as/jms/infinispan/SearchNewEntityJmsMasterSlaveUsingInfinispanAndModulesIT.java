package org.infinispan.test.integration.as.jms.infinispan;


import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.junit.runner.RunWith;

import static org.infinispan.test.integration.as.VersionTestHelper.manifestDependencies;

/**
 * Execute the tests in {@link SearchNewEntityJmsMasterSlaveAndInfinispan} using the modules in JBoss AS to add the
 * required dependencies.
 *
 * @author Davide D'Alto
 * @author Sanne Grinovero
 */
@RunWith(Arquillian.class)
public class SearchNewEntityJmsMasterSlaveUsingInfinispanAndModulesIT extends SearchNewEntityJmsMasterSlaveAndInfinispan {

   @Deployment(name = "master", order = 1)
   public static Archive<?> createDeploymentMaster() throws Exception {
      Archive<?> master = DeploymentJmsMasterSlaveAndInfinispan.createMaster("master");
      addHibernateSearchAndInfinispanDependencies(master);
      return master;
   }

   @Deployment(name = "slave-1", order = 2)
   public static Archive<?> createDeploymentSlave1() throws Exception {
      Archive<?> slave = DeploymentJmsMasterSlaveAndInfinispan.createSlave("slave-1");
      addHibernateSearchAndInfinispanDependencies(slave);
      return slave;
   }

   @Deployment(name = "slave-2", order = 3)
   public static Archive<?> createDeploymentSlave2() throws Exception {
      Archive<?> slave = DeploymentJmsMasterSlaveAndInfinispan.createSlave("slave-2");
      addHibernateSearchAndInfinispanDependencies(slave);
      return slave;
   }

   private static void addHibernateSearchAndInfinispanDependencies(Archive<?> archive) {
      //This is intentionally using the Infinispan "main" slot to verify there are no conflicts between using the Infinispan
      //version included in WildFly by default (usually older than the Infinispan version being built) and the latest Hibernate Search slot,
      //which is being wired up to the Infinispan version being built.
      //So this test makes sure both version of Infinispan can co-exist in this use case.
      archive.add( manifestDependencies( "org.infinispan:main, org.hibernate.search.orm:${hibernate-search.module.slot} services"), "META-INF/MANIFEST.MF" );
   }

}
