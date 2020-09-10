package org.infinispan.xsite.irac;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.Optional;

import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.irac.DefaultIracVersionGenerator;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.TopologyIracVersion;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Unit test for {@link IracEntryVersion}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "unit", testName = "xsite.irac.IracVersionUnitTest")
public class IracVersionUnitTest extends AbstractInfinispanTest {

   private static final String SITE_1 = "site_1";
   private static final String SITE_2 = "site_2";

   private static DefaultIracVersionGenerator newGenerator(String site) {
      DefaultIracVersionGenerator generator = new DefaultIracVersionGenerator("cache-name");
      TestingUtil.inject(generator, mockRpcManager(site), mockGlobalStateManager());
      generator.start();
      return generator;
   }

   private static Transport mockTransport(String siteName) {
      Transport t = Mockito.mock(Transport.class);
      Mockito.doNothing().when(t).checkCrossSiteAvailable();
      Mockito.when(t.localSiteName()).thenReturn(siteName);
      return t;
   }

   private static RpcManager mockRpcManager(String siteName) {
      Transport transport = mockTransport(siteName);
      RpcManager rpcManager = Mockito.mock(RpcManager.class);
      Mockito.when(rpcManager.getTransport()).thenReturn(transport);
      return rpcManager;
   }

   private static CacheTopology mockCacheTopology() {
      CacheTopology t = Mockito.mock(CacheTopology.class);
      Mockito.when(t.getPhase()).thenReturn(CacheTopology.Phase.NO_REBALANCE);
      return t;
   }

   private static GlobalStateManager mockGlobalStateManager() {
      GlobalStateManager manager = Mockito.mock(GlobalStateManager.class);
      Mockito.doNothing().when(manager).writeScopedState(Mockito.any(ScopedPersistentState.class));
      Mockito.when(manager.readScopedState(Mockito.anyString())).thenReturn(Optional.empty());
      return manager;
   }

   private static void assertSiteVersion(IracEntryVersion entryVersion, String site, int topologyId, long version) {
      assertNotNull(entryVersion);
      TopologyIracVersion iracVersion = entryVersion.toMap().get(site);
      assertNotNull(iracVersion);
      assertEquals(topologyId, iracVersion.getTopologyId());
      assertEquals(version, iracVersion.getVersion());
   }

   private static void assertNoSiteVersion(IracEntryVersion entryVersion, String site) {
      assertNotNull(entryVersion);
      TopologyIracVersion iracVersion = entryVersion.toMap().get(site);
      assertNull(iracVersion);
   }

   private static void triggerTopologyEvent(DefaultIracVersionGenerator generator) {
      generator.onTopologyChange(mockCacheTopology());
   }

   public void testEquals() {
      IracMetadata m1 = newGenerator(SITE_1).generateNewMetadata(0);
      IracMetadata m2 = newGenerator(SITE_1).generateNewMetadata(0);

      assertEquals(InequalVersionComparisonResult.EQUAL, m1.getVersion().compareTo(m2.getVersion()));
      assertEquals(InequalVersionComparisonResult.EQUAL, m2.getVersion().compareTo(m1.getVersion()));
   }

   public void testCompareDifferentTopology() {
      DefaultIracVersionGenerator g1 = newGenerator(SITE_1);

      IracMetadata m1 = g1.generateNewMetadata(0); // (1,0)
      triggerTopologyEvent(g1);

      IracMetadata m2 = g1.generateNewMetadata(0); //(1+,0)

      assertSiteVersion(m1.getVersion(), SITE_1, 1, 1);
      assertNoSiteVersion(m1.getVersion(), SITE_2);

      assertSiteVersion(m2.getVersion(), SITE_1, 2, 1);
      assertNoSiteVersion(m2.getVersion(), SITE_2);

      // we have m1=(1,0) and m2=(1+,0)
      assertEquals(InequalVersionComparisonResult.BEFORE, m1.getVersion().compareTo(m2.getVersion()));
      assertEquals(InequalVersionComparisonResult.AFTER, m2.getVersion().compareTo(m1.getVersion()));
   }

   public void testCompareSameTopology() {
      DefaultIracVersionGenerator g1 = newGenerator(SITE_1);

      IracMetadata m1 = g1.generateNewMetadata(0); // (1,0)
      IracMetadata m2 = g1.generateNewMetadata(0); // (2,0)

      assertSiteVersion(m1.getVersion(), SITE_1, 1, 1);
      assertNoSiteVersion(m1.getVersion(), SITE_2);

      assertSiteVersion(m2.getVersion(), SITE_1, 1, 2);
      assertNoSiteVersion(m2.getVersion(), SITE_2);

      assertEquals(InequalVersionComparisonResult.BEFORE, m1.getVersion().compareTo(m2.getVersion()));
      assertEquals(InequalVersionComparisonResult.AFTER, m2.getVersion().compareTo(m1.getVersion()));
   }

   public void testCausality() {
      DefaultIracVersionGenerator g1 = newGenerator(SITE_1);
      DefaultIracVersionGenerator g2 = newGenerator(SITE_2);

      IracMetadata m2 = g2.generateNewMetadata(0); // (0,1)

      assertNoSiteVersion(m2.getVersion(), SITE_1);
      assertSiteVersion(m2.getVersion(), SITE_2, 1, 1);


      g1.updateVersion(0, m2.getVersion());
      IracMetadata m1 = g1.generateNewMetadata(0); // (1,1)

      assertSiteVersion(m1.getVersion(), SITE_1, 1, 1);
      assertSiteVersion(m1.getVersion(), SITE_2, 1, 1);

      //we have m1=(1,1) and m2=(0,1)
      assertEquals(InequalVersionComparisonResult.BEFORE, m2.getVersion().compareTo(m1.getVersion()));
      assertEquals(InequalVersionComparisonResult.AFTER, m1.getVersion().compareTo(m2.getVersion()));
   }

   public void testConflictSameTopology() {
      DefaultIracVersionGenerator g1 = newGenerator(SITE_1);
      DefaultIracVersionGenerator g2 = newGenerator(SITE_2);

      IracMetadata m1 = g1.generateNewMetadata(0); // (1,0)
      IracMetadata m2 = g2.generateNewMetadata(0); // (0,1)

      assertEquals(SITE_1, m1.getSite());
      assertEquals(SITE_2, m2.getSite());

      assertSiteVersion(m1.getVersion(), SITE_1, 1, 1);
      assertNoSiteVersion(m1.getVersion(), SITE_2);

      assertNoSiteVersion(m2.getVersion(), SITE_1);
      assertSiteVersion(m2.getVersion(), SITE_2, 1, 1);

      //we have a conflict: (1,0) vs (0,1)

      assertEquals(InequalVersionComparisonResult.CONFLICTING, m1.getVersion().compareTo(m2.getVersion()));
   }

   public void testConflictDifferentTopology() {
      DefaultIracVersionGenerator g1 = newGenerator(SITE_1);
      DefaultIracVersionGenerator g2 = newGenerator(SITE_2);

      g2.generateNewMetadata(0); //(0,1)
      IracMetadata m2 = g2.generateNewMetadata(0); //(0,2)

      g1.updateVersion(0, m2.getVersion());
      IracMetadata m1 = g1.generateNewMetadata(0); //(1,2)

      triggerTopologyEvent(g2);
      m2 = g2.generateNewMetadata(0); //(0,1+)

      //we should have a conflict m1=(1,2) & m2=(0,1+)

      assertSiteVersion(m1.getVersion(), SITE_1, 1, 1);
      assertSiteVersion(m1.getVersion(), SITE_2, 1, 2);

      assertNoSiteVersion(m2.getVersion(), SITE_1);
      assertSiteVersion(m2.getVersion(), SITE_2, 2, 1);

      assertEquals(InequalVersionComparisonResult.CONFLICTING, m1.getVersion().compareTo(m2.getVersion()));
      assertEquals(InequalVersionComparisonResult.CONFLICTING, m2.getVersion().compareTo(m1.getVersion()));
   }

   public void testNoConflictDifferentTopology() {
      DefaultIracVersionGenerator g1 = newGenerator(SITE_1);
      DefaultIracVersionGenerator g2 = newGenerator(SITE_2);

      IracMetadata m3 = g2.generateNewMetadata(0); //(0,1)
      IracMetadata m2 = g2.generateNewMetadata(0); //(0,2)

      g1.updateVersion(0, m2.getVersion());
      IracMetadata m1 = g1.generateNewMetadata(0); //(1,2)

      //we have m1=(1,2) & m3=(0,1).

      assertSiteVersion(m1.getVersion(), SITE_1, 1, 1);
      assertSiteVersion(m1.getVersion(), SITE_2, 1, 2);

      assertNoSiteVersion(m3.getVersion(), SITE_1);
      assertSiteVersion(m3.getVersion(), SITE_2, 1, 1);

      assertEquals(InequalVersionComparisonResult.AFTER, m1.getVersion().compareTo(m3.getVersion()));
      assertEquals(InequalVersionComparisonResult.BEFORE, m3.getVersion().compareTo(m1.getVersion()));
   }
}
