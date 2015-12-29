package org.infinispan.server.hotrod

import java.lang.reflect.Method

import org.infinispan.commons.api.BasicCacheContainer.DEFAULT_CACHE_NAME
import org.infinispan.configuration.cache.{ConfigurationBuilder, CacheMode}
import org.infinispan.partitionhandling.BasePartitionHandlingTest
import org.infinispan.partitionhandling.BasePartitionHandlingTest.PartitionDescriptor
import org.infinispan.server.core.test.ServerTestingUtil._
import org.infinispan.server.hotrod.Constants._
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.test.{HotRodClient, HotRodTestingUtil}
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.test.AbstractCacheTest.CleanupPhase
import org.infinispan.test.AbstractInfinispanTest.Condition
import org.infinispan.test.TestingUtil
import org.infinispan.test.fwk.TransportFlags
import org.testng.annotations.{AfterClass, BeforeClass, Test}

import scala.collection.mutable.ListBuffer

@Test(groups = Array("functional"), testName = "server.hotrod.HotRodViewMergeTest")
class HotRodMergeTest extends BasePartitionHandlingTest {

   numMembersInCluster = 2
   cacheMode = CacheMode.DIST_SYNC
   cleanup = CleanupPhase.AFTER_TEST

   private[this] val servers = new ListBuffer[HotRodServer]()
   private[this] var client: HotRodClient = _

   @BeforeClass(alwaysRun = true)
   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createBeforeClass() {
      super.createBeforeClass()

      var nextServerPort = serverPort
      for (i <- 0 until numMembersInCluster) {
         servers += HotRodTestingUtil.startHotRodServer(cacheManagers.get(i), nextServerPort)
         nextServerPort += 50
      }

      client = new HotRodClient("127.0.0.1", servers.head.getPort, "", 60, 21)
      TestingUtil.waitForRehashToComplete(cache(0), cache(1))
   }

   @AfterClass(alwaysRun = true)
   override protected def destroy(): Unit = {
      try {
         killClient(client)
         servers.foreach(killServer(_))
      } finally {
         super.destroy()
      }
   }

   @Test(enabled=false) // to avoid TestNG picking it up as a test
   override protected def createCacheManagers(): Unit = {
      val dcc = hotRodCacheConfiguration(new ConfigurationBuilder())
      dcc.clustering.cacheMode(cacheMode).hash().numOwners(1)
      createClusteredCaches(numMembersInCluster, dcc, new TransportFlags().withFD(true).withMerge(true))
      waitForClusterToForm()
   }

   def testNewTopologySentAfterCleanMerge(m: Method) {
      TestingUtil.waitForRehashToComplete(caches())
      val initialTopology = advancedCache(0).getRpcManager.getTopologyId

      expectCompleteTopology(client, initialTopology)
      val p0 = new PartitionDescriptor(0)
      val p1 = new PartitionDescriptor(1)
      splitCluster(p0.getNodes, p1.getNodes)
      TestingUtil.waitForRehashToComplete(cache(p1.node(0)))
      TestingUtil.waitForRehashToComplete(cache(p0.node(0)))
      expectPartialTopology(client, initialTopology + 1)
      partition(0).merge(partition(1))
      eventuallyExpectCompleteTopology(client, initialTopology + 6)
   }

   def testNewTopologySentAfterOverlappingMerge(m: Method) {
      TestingUtil.waitForRehashToComplete(caches())
      val initialTopology = advancedCache(0).getRpcManager.getTopologyId
      expectCompleteTopology(client, initialTopology)
      val p1 = new PartitionDescriptor(0)
      isolatePartition(p1.getNodes)
      TestingUtil.waitForRehashToComplete(cache(p1.node(0)))
      eventuallyExpectPartialTopology(client, initialTopology + 1)

      partition(0).merge(partition(1))
      eventuallyExpectCompleteTopology(client, initialTopology + 2)
   }


   private def eventuallyExpectCompleteTopology(c: HotRodClient, expectedTopologyId: Int): Unit = {
      eventually(new Condition {
         override def isSatisfied: Boolean = {
            val resp = c.ping(INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
            assertStatus(resp, Success)
            if (!resp.topologyResponse.isDefined || (resp.topologyResponse.get.topologyId < expectedTopologyId)) {
               return false
            }
            assertHashTopology20Received(resp.topologyResponse.get, servers.toList, DEFAULT_CACHE_NAME, expectedTopologyId)
            return true
         }
      })
   }

   private def expectCompleteTopology(c: HotRodClient, expectedTopologyId: Int): Unit = {
      val resp = c.ping(INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
      assertStatus(resp, Success)
      assertHashTopology20Received(resp.topologyResponse.get, servers.toList, DEFAULT_CACHE_NAME, expectedTopologyId)
   }

   private def eventuallyExpectPartialTopology(c: HotRodClient, expectedTopologyId: Int): Unit = {
      eventually(new Condition {
         override def isSatisfied: Boolean = {
            val resp = c.ping(INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
            assertStatus(resp, Success)
            if (!resp.topologyResponse.isDefined || (resp.topologyResponse.get.topologyId < expectedTopologyId)) {
               return false
            }
            assertHashTopology20Received(resp.topologyResponse.get, List(servers.head), DEFAULT_CACHE_NAME, expectedTopologyId)
            return true
         }
      })
   }

   private def expectPartialTopology(c: HotRodClient, expectedTopologyId: Int): Unit = {
      val resp = c.ping(INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
      assertStatus(resp, Success)
      assertHashTopology20Received(resp.topologyResponse.get, List(servers.head), DEFAULT_CACHE_NAME, expectedTopologyId)
   }

}
