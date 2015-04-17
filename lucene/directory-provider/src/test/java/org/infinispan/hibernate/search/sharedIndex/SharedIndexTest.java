package org.infinispan.hibernate.search.sharedIndex;

import org.apache.lucene.search.Query;
import org.hibernate.Transaction;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.test.util.FullTextSessionBuilder;
import org.infinispan.hibernate.search.ClusterSharedConnectionProvider;
import org.infinispan.hibernate.search.spi.InfinispanDirectoryProvider;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;

import static org.infinispan.hibernate.search.ClusterTestHelper.createClusterNode;
import static org.infinispan.hibernate.search.ClusterTestHelper.waitMembersCount;
import static org.junit.Assert.assertEquals;

/**
 * Test to verify HSEARCH-926
 *
 * @author Zach Kurey
 */

public class SharedIndexTest {
   FullTextSessionBuilder node;
   HashSet<Class<?>> entityTypes;

   @Test
   public void testSingleResultFromDeviceIndex() {
      assertEquals(1, clusterSize(node, Toaster.class));
      // index an entity:
      {
         FullTextSession fullTextSession = node.openFullTextSession();
         Transaction transaction = fullTextSession.beginTransaction();
         Toaster toaster = new Toaster("A1");
         fullTextSession.save(toaster);
         transaction.commit();
         fullTextSession.close();
         verifyResult(node);
      }
   }

   private void verifyResult(FullTextSessionBuilder node) {
      FullTextSession fullTextSession = node.openFullTextSession();
      try {
         Transaction transaction = fullTextSession.beginTransaction();
         QueryBuilder queryBuilder = fullTextSession.getSearchFactory().buildQueryBuilder()
               .forEntity(Toaster.class).get();
         Query query = queryBuilder.keyword().onField("serialNumber").matching("A1").createQuery();
         List list = fullTextSession.createFullTextQuery(query).list();
         assertEquals(1, list.size());
         Device device = (Device) list.get(0);

         assertEquals("GE", device.manufacturer);
         transaction.commit();
      } finally {
         fullTextSession.close();
      }
   }

   @Before
   public void setUp() throws Exception {
      entityTypes = new HashSet<Class<?>>();
      entityTypes.add(Device.class);
      entityTypes.add(Robot.class);
      entityTypes.add(Toaster.class);
      node = createClusterNode(entityTypes, true);
      waitMembersCount(node, Toaster.class, 1);
   }

   @After
   public void tearDown() throws Exception {
      if (node != null) {
         node.close();
      }
   }

   @BeforeClass
   public static void prepareConnectionPool() {
      ClusterSharedConnectionProvider.realStart();
   }

   @AfterClass
   public static void shutdownConnectionPool() {
      ClusterSharedConnectionProvider.realStop();
   }

   /**
    * Counts the number of nodes in the cluster on this node
    *
    * @param node the FullTextSessionBuilder representing the current node
    * @return
    */
   protected int clusterSize(FullTextSessionBuilder node, Class<?> entityType) {
      SearchIntegrator integrator = node.getSearchFactory().unwrap(SearchIntegrator.class);
      EntityIndexBinding indexBinding = integrator.getIndexBinding(Toaster.class);
      DirectoryBasedIndexManager indexManager = (DirectoryBasedIndexManager) indexBinding.getIndexManagers()[0];
      InfinispanDirectoryProvider directoryProvider = (InfinispanDirectoryProvider) indexManager.getDirectoryProvider();
      EmbeddedCacheManager cacheManager = directoryProvider.getCacheManager();
      List<Address> members = cacheManager.getMembers();
      return members.size();
   }
}
