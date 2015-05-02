package org.infinispan.test.integration.as.jms;

import org.infinispan.test.integration.as.jms.controller.StatisticsController;
import org.infinispan.test.integration.as.jms.model.RegisteredMember;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.junit.InSequence;
import org.jboss.shrinkwrap.api.Archive;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;

import static org.infinispan.test.integration.as.VersionTestHelper.addHibernateSearchManifestDependencies;
import static org.junit.Assert.assertTrue;

/**
 * Test the the combination of JMS+Infinispan as backend and the use of Infinispan as second level cache.
 *
 * @author Davide D'Alto
 * @author Sanne Grinovero
 */
@RunWith(Arquillian.class)
public class SearchNewEntityJmsMasterSlaveUsingInfinispanAs2ndCacheAndModulesIT extends SearchNewEntityJmsMasterSlave {

   @Inject
   StatisticsController stats;

   @Test
   @InSequence(1005)
   @OperateOnDeployment("slave-1")
   public void secondLevelCacheShouldBeActive() throws Exception {
      RegisteredMember cachedMember = memberRegistration.getNewMember();
      cachedMember.setName("Johnny Cached");
      memberRegistration.register();

      // Cache the result
      memberRegistration.findById(cachedMember.getId());
      memberRegistration.findById(cachedMember.getId());
      memberRegistration.findById(cachedMember.getId());

      long secondLevelCacheMissCount = stats.getStatistics().getSecondLevelCacheMissCount();
      assertTrue("Second level cache not enabled", secondLevelCacheMissCount > 0);
   }

   @Deployment(name = "master", order = 1)
   public static Archive<?> createDeploymentMaster() throws Exception {
      Archive<?> master = DeploymentJmsMasterSlaveAndInfinispanAs2ndLevelCache.createMaster("master");
      addHibernateSearchManifestDependencies(master);
      return master;
   }

   @Deployment(name = "slave-1", order = 2)
   public static Archive<?> createDeploymentSlave1() throws Exception {
      Archive<?> slave = DeploymentJmsMasterSlaveAndInfinispanAs2ndLevelCache.createSlave("slave-1");
      addHibernateSearchManifestDependencies(slave);
      return slave;
   }

   @Deployment(name = "slave-2", order = 3)
   public static Archive<?> createDeploymentSlave2() throws Exception {
      Archive<?> slave = DeploymentJmsMasterSlaveAndInfinispanAs2ndLevelCache.createSlave("slave-2");
      addHibernateSearchManifestDependencies(slave);
      return slave;
   }

}
