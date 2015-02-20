/*
 * Hibernate Search, full-text search for your domain model
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.integration.as.jms;

import org.infinispan.test.integration.as.jms.controller.RegistrationController;
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
 * In a JMS Master/Slave configuration, every node should be able to find entities created by some other nodes after the
 * synchronization succeed.
 * <p/>
 * Search dependencies are not added to the archives.
 *
 * @author Davide D'Alto <davide@hibernate.org>
 */
public abstract class SearchNewEntityJmsMasterSlave {

   /**
    * Affects how often the Master and Slave directories should start the refresh copy work
    */
   static final int REFRESH_PERIOD_IN_SEC = 2;

   /**
    * Idle loop to wait for results to be transmitted
    */
   private static final int SLEEP_TIME_FOR_SYNCHRONIZATION = 50;

   /**
    * Multiplier on top of REFRESH_PERIOD_IN_SEC we can wait before considering the test failed.
    */
   private static final int MAX_PERIOD_RETRIES = 5;

   private static final int MAX_SEARCH_ATTEMPTS = (MAX_PERIOD_RETRIES * REFRESH_PERIOD_IN_SEC * 1000 / SLEEP_TIME_FOR_SYNCHRONIZATION);

   @Inject
   RegistrationController memberRegistration;

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
      newMember.setEmail("dd@slave1.fake.email");
      memberRegistration.register();

      assertNotNull("A registered member should have an ID", newMember.getId());
   }

   @Test
   @InSequence(2)
   @OperateOnDeployment("slave-2")
   public void registerNewMemberOnSlave2() throws Exception {
      RegisteredMember newMember = memberRegistration.getNewMember();
      assertNull("A non registered member should have null ID", newMember.getId());

      newMember.setName("Peter O'Tall");
      newMember.setEmail("po@slave2.fake.email");
      memberRegistration.register();

      assertNotNull("A registered member should have an ID", newMember.getId());
   }

   @Test
   @InSequence(3)
   @OperateOnDeployment("master")
   public void registerNewMemberOnMaster() throws Exception {
      RegisteredMember newMember = memberRegistration.getNewMember();
      assertNull("A non registered member should have null ID", newMember.getId());

      newMember.setName("Richard Mayhew");
      newMember.setEmail("rm@master.fake.email");
      memberRegistration.register();

      assertNotNull("A registered member should have an ID", newMember.getId());
   }

   @Test
   @InSequence(4)
   @OperateOnDeployment("slave-1")
   public void searchNewMembersAfterSynchronizationOnSlave1() throws Exception {
      assertSearchResult("Davide D'Alto", search("Davide"));
      assertSearchResult("Peter O'Tall", search("Peter"));
      assertSearchResult("Richard Mayhew", search("Richard"));
   }

   @Test
   @InSequence(5)
   @OperateOnDeployment("slave-2")
   public void searchNewMembersAfterSynchronizationOnSlave2() throws Exception {
      assertSearchResult("Davide D'Alto", search("Davide"));
      assertSearchResult("Peter O'Tall", search("Peter"));
      assertSearchResult("Richard Mayhew", search("Richard"));
   }

   @Test
   @InSequence(6)
   @OperateOnDeployment("master")
   public void searchNewMembersAfterSynchronizationOnMaster() throws Exception {
      assertSearchResult("Davide D'Alto", search("Davide"));
      assertSearchResult("Peter O'Tall", search("Peter"));
      assertSearchResult("Richard Mayhew", search("Richard"));
   }

   private void assertSearchResult(String expectedResult, List<RegisteredMember> results) {
      assertEquals("Unexpected number of results from search", 1, results.size());
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
