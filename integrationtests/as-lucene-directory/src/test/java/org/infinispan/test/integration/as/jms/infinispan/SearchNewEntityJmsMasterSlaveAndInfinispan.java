package org.infinispan.test.integration.as.jms.infinispan;

import org.infinispan.test.integration.as.jms.controller.RegistrationController;
import org.infinispan.test.integration.as.jms.infinispan.controller.MembersCache;
import org.infinispan.test.integration.as.jms.model.RegisteredMember;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.junit.InSequence;
import org.junit.Test;

import javax.inject.Inject;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Test that the JMS backend can be used at the same time with Infinispan.
 * <p/>
 * Search dependencies are not added to the archives.
 *
 * @author Davide D'Alto <davide@hibernate.org>
 */
public abstract class SearchNewEntityJmsMasterSlaveAndInfinispan {

   /**
    * Idle loop to wait for results to be transmitted
    */
   private static final int SLEEP_TIME_FOR_SYNCHRONIZATION = 50;

   /**
    * Multiplier to how long we can wait before considering the test failed.
    */
   private static final int MAX_PERIOD_RETRIES = 5;

   private static final int MAX_SEARCH_ATTEMPTS = (MAX_PERIOD_RETRIES * 1000 / SLEEP_TIME_FOR_SYNCHRONIZATION);

   @Inject
   RegistrationController memberRegistration;

   @Inject
   MembersCache cache;

   @Test
   @InSequence(0)
   @OperateOnDeployment("master")
   public void deleteExistingMembers() throws Exception {
      int deletedMembers = memberRegistration.deleteAllMembers();
      assertEquals("At the start of the test there should be no members", 0, deletedMembers);
   }

   @Test
   @InSequence(1)
   @OperateOnDeployment("slave-1")
   public void registerNewMemberOnSlave1() throws Exception {
      RegisteredMember newMember = memberRegistration.getNewMember();
      assertNull("A non registered member should have null ID", newMember.getId());

      newMember.setName("Davide D'Alto");
      newMember.setEmail("slave1@email");
      memberRegistration.register();

      assertNotNull("A registered member should have an ID", newMember.getId());

      // Add new member in the cache
      cache.put("slave-1", newMember.getName());
   }

   @Test
   @InSequence(2)
   @OperateOnDeployment("slave-2")
   public void registerNewMemberOnSlave2() throws Exception {
      RegisteredMember newMember = memberRegistration.getNewMember();
      assertNull("A non registered member should have null ID", newMember.getId());

      newMember.setName("Peter O'Tall");
      newMember.setEmail("slave2@email");
      memberRegistration.register();

      assertNotNull("A registered member should have an ID", newMember.getId());

      // Add new member in the cache
      cache.put("slave-2", newMember.getName());

      // Read slave-1 member from the cache
      assertEquals("Davide D'Alto", cache.get("slave-1"));
   }

   @Test
   @InSequence(3)
   @OperateOnDeployment("master")
   public void searchNewMembersAfterSynchronizationOnMaster() throws Exception {
      assertSearchResult("Davide D'Alto", search("Davide"));
      assertSearchResult("Peter O'Tall", search("Peter"));

      // Read users from the cache
      assertEquals("Missing cache entry", "Davide D'Alto", cache.get("slave-1"));
      assertEquals("Missing cache entry", "Peter O'Tall", cache.get("slave-2"));
   }

   private void assertSearchResult(String expectedResult, List<RegisteredMember> results) {
      assertEquals("Unexpected number of results, expected  <" + expectedResult + ">", 1, results.size());
      assertEquals("Unexpected result from search", expectedResult, results.get(0).getName());
   }

   private void waitForIndexSynchronization() throws InterruptedException {
      Thread.sleep(SLEEP_TIME_FOR_SYNCHRONIZATION);
   }

   private List<RegisteredMember> search(String name) throws InterruptedException {
      List<RegisteredMember> results = memberRegistration.search(name);

      int attempts = 0;
      while (results.size() == 0 && attempts < MAX_SEARCH_ATTEMPTS) {
         attempts++;
         waitForIndexSynchronization();
         results = memberRegistration.search(name);
      }
      return results;
   }

}
