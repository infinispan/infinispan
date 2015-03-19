package org.infinispan.hibernate.search;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.hibernate.Transaction;
import org.hibernate.search.FullTextQuery;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.test.util.FullTextSessionBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.infinispan.hibernate.search.ClusterTestHelper.clusterSize;
import static org.infinispan.hibernate.search.ClusterTestHelper.createClusterNode;
import static org.infinispan.hibernate.search.ClusterTestHelper.waitMembersCount;

/**
 * In this test we initially start a master node which will stay alive for the full test duration and constantly
 * indexing new entities.
 * <p/>
 * After that we add and remove additional new nodes, still making more index changes checking that each node is always
 * able to see changes as soon as committed by the main node; this results in a very stressfull test as the cluster
 * topology is changed at each step (though it doesn't rehash as it's replicating).
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
public class LiveRunningTest {

   private static final int TEST_RUNS = 17;
   private static final int MAX_SLAVES = 5;

   private static HashSet<Class<?>> entityTypes;

   private final FullTextSessionBuilder master = createClusterNode(entityTypes, true);
   private final List<FullTextSessionBuilder> slaves = new LinkedList<>();

   private boolean growCluster = true;

   private int storedEmailsCount = 0;

   @Test
   public void liveRun() {
      try {
         for (int i = 0; i < TEST_RUNS; i++) {
            writeOnMaster();
            adjustSlavesNumber(i);
            assertViews();
         }
      } finally {
         master.close();
         for (FullTextSessionBuilder slave : slaves) {
            slave.close();
         }
      }
   }

   private void assertViews() {
      assertView(master);
      for (FullTextSessionBuilder slave : slaves) {
         assertView(slave);
      }
   }

   private void assertView(FullTextSessionBuilder node) {
      assertEquals(slaves.size() + 1, clusterSize(node, SimpleEmail.class));
      FullTextSession session = node.openFullTextSession();
      try {
         FullTextQuery fullTextQuery = session.createFullTextQuery(new MatchAllDocsQuery());
         int resultSize = fullTextQuery.getResultSize();
         assertEquals(storedEmailsCount, resultSize);
      } finally {
         session.close();
      }
   }

   private void adjustSlavesNumber(int i) {
      if (growCluster) {
         if (slaves.size() >= MAX_SLAVES) {
            growCluster = false;
         } else {
            slaves.add(createClusterNode(entityTypes, false));
         }
      } else {
         if (slaves.size() == 0) {
            growCluster = true;
         } else {
            FullTextSessionBuilder sessionBuilder = slaves.remove(0);
            sessionBuilder.close();
         }
      }
      waitForAllJoinsCompleted();
   }

   private void writeOnMaster() {
      FullTextSession fullTextSession = master.openFullTextSession();
      try {
         Transaction transaction = fullTextSession.beginTransaction();
         SimpleEmail simpleEmail = new SimpleEmail();
         simpleEmail.to = "outher space";
         simpleEmail.message = "anybody out there?";
         fullTextSession.save(simpleEmail);
         transaction.commit();
         storedEmailsCount++;
      } finally {
         fullTextSession.close();
      }
   }

   private void waitForAllJoinsCompleted() {
      int expectedSize = slaves.size() + 1;
      waitMembersCount(master, SimpleEmail.class, expectedSize);
      for (FullTextSessionBuilder slave : slaves) {
         waitMembersCount(slave, SimpleEmail.class, expectedSize);
      }
   }

   @BeforeClass
   public static void prepareConnectionPool() {
      entityTypes = new HashSet<Class<?>>();
      entityTypes.add(SimpleEmail.class);
      ClusterSharedConnectionProvider.realStart();
   }

   @AfterClass
   public static void shutdownConnectionPool() {
      ClusterSharedConnectionProvider.realStop();
   }

}
