package org.infinispan.xsite.irac.statetransfer;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.infinispan.xsite.XSiteAdminOperations;
import org.infinispan.xsite.irac.ManualIracManager;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * State transfer test for IRAC
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@Test(groups = "functional", testName = "xsite.irac.statetransfer.IracStateTransferTest")
public class IracStateTransferTest extends AbstractMultipleSitesTest {

   private final IracManagerHolder[] iracManagers;

   public IracStateTransferTest() {
      iracManagers = new IracManagerHolder[defaultNumberOfSites()];
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      ConfigurationBuilder builder = super.defaultConfigurationForSite(siteIndex);
      builder.sites().addBackup()
            .site(siteName(siteIndex == 0 ? 1 : 0))
            .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      return builder;
   }

   @Override
   protected void afterSitesCreated() {
      for (int i = 0; i < defaultNumberOfSites(); ++i) {
         List<ManualIracManager> list = new ArrayList<>(defaultNumberOfNodes());
         for (Cache<?, ?> cache : caches(siteName(i))) {
            list.add(ManualIracManager.wrapCache(cache));
         }
         iracManagers[i] = new IracManagerHolder(list);
      }
   }

   @BeforeMethod(alwaysRun = true)
   @Override
   public void createBeforeMethod() {
      super.createBeforeMethod();
      for (IracManagerHolder holder : iracManagers) {
         holder.iracManagers.forEach(m -> m.disable(ManualIracManager.DisableMode.DROP));
      }
   }

   public void testStateTransfer(Method method) {
      //simple state transfer from site1 -> site2
      final int keys = 8;

      //disconnect site1 from site2
      assertOk(0, op -> op.takeSiteOffline(siteName(1)));
      assertStatus(0, 1, XSiteAdminOperations.OFFLINE);

      //put some data in site1
      for (int i = 0; i < keys; ++i) {
         cache(0, 0).put(k(method, i), v(method, i));
      }

      //check keys
      assertKeys(method, 0, 0, keys);
      assertNoKeys(method, 1, 0, keys);

      //pause xsite requests
      iracManagers[0].iracManagers.forEach(ManualIracManager::enable);

      //state state transfer
      assertOk(0, op -> op.pushState(siteName(1)));
      assertStatus(0, 1, XSiteAdminOperations.ONLINE);
      assertEquals(XSiteStateTransferManager.STATUS_SENDING, getPushStatus(0, 1));
      //with IRAC, the receiving site don't know the difference between an update or state transfer
      assertNull(getSendingSiteName(1));

      //resume xsite requests
      iracManagers[0].iracManagers.forEach(m -> m.disable(ManualIracManager.DisableMode.SEND));

      waitStateTransfer(0, 1);
      assertKeys(method, 0, 0, keys);
      assertKeys(method, 1, 0, keys);
   }

   public void testConflict(Method method) {
      //disconnect both sites
      assertOk(0, op -> op.takeSiteOffline(siteName(1)));
      assertOk(1, op -> op.takeSiteOffline(siteName(0)));
      assertStatus(0, 1, XSiteAdminOperations.OFFLINE);
      assertStatus(1, 0, XSiteAdminOperations.OFFLINE);

      //keys 0-3: only in site1
      //keys 4-7: both sites
      //keys 8-11: only in site2
      //trigger state transfer and we should have all keys in both site (with the same value, site1 wins in conflict resolution)

      for (int i = 0; i < 4; ++i) {
         cache(0, 0).put(k(method, i), v(method, "site1", i));
      }
      assertKeys(method, 0, "site1", 0, 4);
      assertNoKeys(method, 1, 0, 4);

      for (int i = 4; i < 8; ++i) {
         cache(0, 0).put(k(method, i), v(method, "site1", i));
         cache(1, 0).put(k(method, i), v(method, "site2", i));
      }
      assertKeys(method, 0, "site1", 0, 8);
      assertNoKeys(method, 1, 0, 4);
      assertKeys(method, 1, "site2", 4, 8);

      for (int i = 8; i < 12; ++i) {
         cache(1, 0).put(k(method, i), v(method, "site2", i));
      }
      assertKeys(method, 0, "site1", 0, 8);
      assertNoKeys(method, 0, 8, 12);
      assertNoKeys(method, 1, 0, 4);
      assertKeys(method, 1, "site2", 4, 12);

      //pause xsite requests
      iracManagers[0].iracManagers.forEach(ManualIracManager::enable);

      //state state transfer site1 => site2
      assertOk(0, op -> op.pushState(siteName(1)));
      assertStatus(0, 1, XSiteAdminOperations.ONLINE);
      assertEquals(XSiteStateTransferManager.STATUS_SENDING, getPushStatus(0, 1));
      //with IRAC, the receiving site don't know the difference between an update or state transfer
      assertNull(getSendingSiteName(1));

      //resume xsite requests
      iracManagers[0].iracManagers.forEach(m -> m.disable(ManualIracManager.DisableMode.SEND));

      waitStateTransfer(0, 1);

      //site1 keys should overwrite site2 keys
      assertKeys(method, 0, "site1", 0, 8);
      assertNoKeys(method, 0, 8, 12);
      assertKeys(method, 1, "site1", 0, 8);
      assertKeys(method, 1, "site2", 8, 12);

      //pause xsite requests
      iracManagers[1].iracManagers.forEach(ManualIracManager::enable);

      //state state transfer site2 => site1
      assertOk(1, op -> op.pushState(siteName(0)));
      assertStatus(1, 0, XSiteAdminOperations.ONLINE);
      assertEquals(XSiteStateTransferManager.STATUS_SENDING, getPushStatus(1, 0));
      //with IRAC, the receiving site don't know the difference between an update or state transfer
      assertNull(getSendingSiteName(0));

      //resume xsite requests
      iracManagers[1].iracManagers.forEach(m -> m.disable(ManualIracManager.DisableMode.SEND));

      waitStateTransfer(1, 0);

      assertKeys(method, 0, "site1", 0, 8);
      assertKeys(method, 0, "site2", 8, 12);
      assertKeys(method, 1, "site1", 0, 8);
      assertKeys(method, 1, "site2", 8, 12);
   }

   private void assertStatus(int srcSite, int dstSite, String status) {
      assertInSite(siteName(srcSite), c -> assertEquals(status, adminOperations(c).siteStatus(siteName(dstSite))));
   }

   private void assertNoKeys(Method method, int srcSite, int startKey, int endKey) {
      assertInSite(siteName(srcSite), cache -> {
         for (int i = startKey; i < endKey; ++i) {
            assertNull(cache.get(k(method, i)));
         }
      });
   }

   private void assertKeys(Method method, int srcSite, String prefix, int startKey, int endKey) {
      assertInSite(siteName(srcSite), cache -> {
         for (int i = startKey; i < endKey; ++i) {
            assertEquals(v(method, prefix, i), cache.get(k(method, i)));
         }
      });
   }

   private void assertKeys(Method method, int srcSite, int startKey, int endKey) {
      assertInSite(siteName(srcSite), cache -> {
         for (int i = startKey; i < endKey; ++i) {
            assertEquals(v(method, i), cache.get(k(method, i)));
         }
      });
   }

   private void assertOk(int siteIndex, Function<XSiteAdminOperations, String> f) {
      assertEquals(XSiteAdminOperations.SUCCESS, f.apply(adminOperations(siteIndex)));
   }

   private void waitStateTransfer(int srcSite, int dstSite) {
      eventually(() -> "Expected <ok> but was " + getPushStatus(srcSite, dstSite),
            () -> XSiteStateTransferManager.STATUS_OK.equals(getPushStatus(srcSite, dstSite)));
   }

   private String getPushStatus(int srcSite, int dstSite) {
      return adminOperations(srcSite).getPushStateStatus().get(siteName(dstSite));
   }

   private String getSendingSiteName(int dstSite) {
      return adminOperations(dstSite).getSendingSiteName();
   }

   private XSiteAdminOperations adminOperations(int siteIndex) {
      return adminOperations(cache(siteIndex, 0));
   }

   private XSiteAdminOperations adminOperations(Cache<?, ?> cache) {
      return extractComponent(cache, XSiteAdminOperations.class);
   }

   private static class IracManagerHolder {
      private final List<ManualIracManager> iracManagers;


      private IracManagerHolder(List<ManualIracManager> iracManagers) {
         this.iracManagers = iracManagers;
      }
   }
}
