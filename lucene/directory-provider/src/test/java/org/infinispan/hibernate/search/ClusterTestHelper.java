package org.infinispan.hibernate.search;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hibernate.cfg.Environment;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.IndexedTypeSet;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.test.util.FullTextSessionBuilder;
import org.infinispan.hibernate.search.impl.DefaultCacheManagerService;
import org.infinispan.hibernate.search.spi.InfinispanDirectoryProvider;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;

/**
 * Helpers to setup several instances of Hibernate Search using clustering to connect the index, and sharing the same H2
 * database instance.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 */
public final class ClusterTestHelper {

   public enum ExclusiveIndexUse {
      EXCLUSIVE {
         void apply(FullTextSessionBuilder node) {
            node.setProperty(key, "true");
         }
      },
      SHARED {
         void apply(FullTextSessionBuilder node) {
            node.setProperty(key, "false");
         }
      };
      private static final String key = "hibernate.search.default." + org.hibernate.search.cfg.Environment.EXCLUSIVE_INDEX_USE;
      abstract void apply(FullTextSessionBuilder node);
   }

   public enum IndexManagerType {
      TRADITIONAL_DIRECTORYPROVIDER {
         void apply(FullTextSessionBuilder node) {
            node.setProperty("hibernate.search.default.directory_provider", "infinispan");
         }
      },
      DEDICATED_INDEXMANAGER {
         void apply(FullTextSessionBuilder node) {
            node.setProperty("hibernate.search.default.indexmanager", "infinispan");
         }
      };
      abstract void apply(FullTextSessionBuilder node);
   }

   public enum IndexingFlushMode {
      ASYNC_PERIODIC {
         void apply(FullTextSessionBuilder node) {
            node.setProperty("hibernate.search.default.index_flush_interval", "1000");
            node.setProperty("hibernate.search.default.worker.execution", "async");
         }
      },
      ASYNC { // Does this even make sense to allow? ASYNC_PERIODIC seems superior in every aspect.
         void apply(FullTextSessionBuilder node) {
            node.setProperty("hibernate.search.default.worker.execution", "async");
         }
      },
      SYNC {
         void apply(FullTextSessionBuilder node) {
            // It also happens to be the default, but things might change:
            node.setProperty("hibernate.search.default.worker.execution", "sync");
         }
      };
      abstract void apply(FullTextSessionBuilder node);
   }

   private ClusterTestHelper() {
      //not allowed
   }

   /**
    * Create a clustered Hibernate Search instance. Note the configuration used is not optimal for performance, we do
    * this on purpose to make sure we test with an highly fragmented index. The backing CacheManager will be started,
    * but didn't necessarily join the existing nodes.
    *
    * @param entityTypes       the set of indexed classes
    * @param exclusiveIndexUse set to true to enable the EXCLUSIVE_INDEX_USE configuration option
    * @return a started FullTextSessionBuilder
    */
   public static FullTextSessionBuilder createClusterNode(IndexedTypeSet entityTypes, ExclusiveIndexUse exclusiveIndexUse, IndexingFlushMode flushMode) {
      return createClusterNode(entityTypes, exclusiveIndexUse, IndexManagerType.TRADITIONAL_DIRECTORYPROVIDER, flushMode);
   }

   /**
    * As {@link #createClusterNode(IndexedTypeSet, boolean)} but allows more options
    *
    * @param entityTypes               the set of indexed classes
    * @param exclusiveIndexUse         set to true to enable the EXCLUSIVE_INDEX_USE configuration option
    * @param setInfinispanDirectory    set to true to enable the directory_provider setting to 'infinispan'
    * @param setInfinispanIndexManager set to true to enable the indexmanager setting to 'infinispan'
    * @return
    */
   public static FullTextSessionBuilder createClusterNode(IndexedTypeSet entityTypes, ExclusiveIndexUse exclusiveIndexUse, IndexManagerType storageType, IndexingFlushMode flushMode) {
      FullTextSessionBuilder node = new FullTextSessionBuilder();
      // Set the DirectoryProvider or the IndexManager:
      storageType.apply(node);
      // Set async / synch, with or without a periodic flush:
      flushMode.apply(node);
      // fragment on every 13 bytes: don't use this on a real case!
      // only done to make sure we generate lots of small fragments.
      node.setProperty("hibernate.search.default.indexwriter.chunk_size", "13");
      //Override the JGroups configuration to use the testing loopback stack
      node.setProperty(DefaultCacheManagerService.INFINISPAN_TRANSPORT_OVERRIDE_RESOURCENAME, "testing-flush-loopback.xml");
      // this schema is shared across nodes, so don't drop it on shutdown:
      node.setProperty(Environment.HBM2DDL_AUTO, "create");
      // if we should allow aggressive index locking:
      exclusiveIndexUse.apply(node);
      // share the same in-memory database connection pool
      node.setProperty(
            Environment.CONNECTION_PROVIDER,
            org.infinispan.hibernate.search.ClusterSharedConnectionProvider.class.getName()
      );
      for (IndexedTypeIdentifier entityType : entityTypes) {
         node.addAnnotatedClass(entityType.getPojoType());
      }

      return node.build();
   }

   /**
    * delegates {@link #waitMembersCount(FullTextSessionBuilder, IndexedTypeIdentifier, int, long, TimeUnit)} with 10s.
    */
   public static void waitMembersCount(FullTextSessionBuilder node, IndexedTypeIdentifier entityType, int expectedSize) {
      waitMembersCount(node, entityType, expectedSize, 10, TimeUnit.SECONDS);
   }

   /**
    * Wait some time for the cluster to form
    *
    * @param node Node to be checked
    * @param entityType An entity for check
    * @param expectedSize Expected size of the cluster
    * @param timeout Desired timeout
    * @param timeoutUnit Timeout units
     */
   public static void waitMembersCount(FullTextSessionBuilder node, IndexedTypeIdentifier entityType, int expectedSize, long timeout, TimeUnit timeoutUnit) {
      long endTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, timeoutUnit);
      int currentSize = 0;
      do {
         if (System.currentTimeMillis() > endTime) {
            throw new AssertionError("Timeout when waiting for desired number of nodes. Expected: " + expectedSize + ", got: " + currentSize);
         }
         Thread.yield();
         currentSize = clusterSize(node, entityType);
      } while(currentSize != expectedSize);
   }

   /**
    * Counts the number of nodes in the cluster on this node
    *
    * @param node the FullTextSessionBuilder representing the current node
    * @return the number of nodes as seen by the current node
    */
   public static int clusterSize(FullTextSessionBuilder node, IndexedTypeIdentifier entityType) {
      SearchIntegrator integrator = node.getSearchFactory().unwrap(SearchIntegrator.class);
      EntityIndexBinding indexBinding = integrator.getIndexBinding(entityType);
      DirectoryBasedIndexManager indexManager = (DirectoryBasedIndexManager) indexBinding.getIndexManagerSelector().all().iterator().next();
      InfinispanDirectoryProvider directoryProvider = (InfinispanDirectoryProvider) indexManager.getDirectoryProvider();
      EmbeddedCacheManager cacheManager = directoryProvider.getCacheManager();
      List<Address> members = cacheManager.getMembers();
      return members.size();
   }

}
