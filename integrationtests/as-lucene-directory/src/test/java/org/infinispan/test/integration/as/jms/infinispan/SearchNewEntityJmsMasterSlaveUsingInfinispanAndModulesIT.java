package org.infinispan.test.integration.as.jms.infinispan;


import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.junit.runner.RunWith;

import static org.infinispan.test.integration.as.VersionTestHelper.addDependencyToSearchModule;

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
      addDependencies(master);
      return master;
   }

   @Deployment(name = "slave-1", order = 2)
   public static Archive<?> createDeploymentSlave1() throws Exception {
      Archive<?> slave = DeploymentJmsMasterSlaveAndInfinispan.createSlave("slave-1");
      addDependencies(slave);
      return slave;
   }

   @Deployment(name = "slave-2", order = 3)
   public static Archive<?> createDeploymentSlave2() throws Exception {
      Archive<?> slave = DeploymentJmsMasterSlaveAndInfinispan.createSlave("slave-2");
      addDependencies(slave);
      return slave;
   }

   private static void addDependencies(Archive<?> archive) {
      addDependencyToSearchModule(archive, "org.infinispan");
   }
}
