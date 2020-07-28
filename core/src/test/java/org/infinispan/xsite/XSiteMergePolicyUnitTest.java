package org.infinispan.xsite;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Random;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.xsite.spi.AlwaysRemoveXSiteEntryMergePolicy;
import org.infinispan.xsite.spi.DefaultXSiteEntryMergePolicy;
import org.infinispan.xsite.spi.PreferNonNullXSiteEntryMergePolicy;
import org.infinispan.xsite.spi.PreferNullXSiteEntryMergePolicy;
import org.infinispan.xsite.spi.SiteEntry;
import org.infinispan.xsite.spi.XSiteEntryMergePolicy;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for all provided {@link XSiteEntryMergePolicy}.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@Test(groups = "unit", testName = "xsite.XSiteMergePolicyUnitTest")
public class XSiteMergePolicyUnitTest extends AbstractInfinispanTest {

   private static final Random RANDOM = new Random(System.currentTimeMillis());

   private static Sync defaultMergePolicy() {
      return new Sync(DefaultXSiteEntryMergePolicy.getInstance());
   }

   public static Sync preferNonNullMergePolicy() {
      return new Sync(PreferNonNullXSiteEntryMergePolicy.getInstance());
   }

   public static Sync preferNullMergePolicy() {
      return new Sync(PreferNullXSiteEntryMergePolicy.getInstance());
   }

   public static Sync alwaysRemoveMergePolicy() {
      return new Sync(AlwaysRemoveXSiteEntryMergePolicy.getInstance());
   }

   @DataProvider(name = "alwaysRemoveData")
   private static Object[][] alwaysRemoveDataProvider() {
      //it create a new null entry with the lower site name (lower lexicographically)
      SiteEntryMode[] values = SiteEntryMode.values();
      Object[][] result = new Object[values.length * 2][3];
      int i = 0;
      for (SiteEntryMode mode : values) {
         result[i] = createInputs(mode, "LON", "NYC");
         result[i++][2] = newNullSiteEntry("LON");

         result[i] = createInputs(mode, "1NYC", "2LON");
         result[i++][2] = newNullSiteEntry("1NYC");
      }
      return result;
   }

   @DataProvider(name = "defaultData")
   private static Object[][] defaultDataProvider() {
      //by default, it resolves to s1 (lower lexicographically)
      SiteEntryMode[] values = SiteEntryMode.values();
      Object[][] result = new Object[values.length * 2][3];
      int i = 0;
      for (SiteEntryMode mode : values) {
         result[i] = createInputs(mode, "LON", "NYC");
         result[i][2] = result[i][0];
         ++i;
         result[i] = createInputs(mode, "1NYC", "2LON");
         result[i][2] = result[i][0];
         ++i;
      }
      return result;
   }

   @DataProvider(name = "preferNullData")
   private static Object[][] preferNullDataProvider() {
      //it resolves to the null entry (if any), otherwise it resolves to s1 (lower lexicographically)
      SiteEntryMode[] values = SiteEntryMode.values();
      Object[][] result = new Object[values.length * 2][3];
      int i = 0;
      for (SiteEntryMode mode : values) {
         result[i] = createInputs(mode, "LON", "NYC");
         result[i][2] = mode == SiteEntryMode.S2_NULL ? result[i][1] : result[i][0];
         ++i;
         result[i] = createInputs(mode, "1NYC", "2LON");
         result[i][2] = mode == SiteEntryMode.S2_NULL ? result[i][1] : result[i][0];
         ++i;
      }
      return result;
   }

   @DataProvider(name = "preferNonNullData")
   private static Object[][] preferNonNullDataProvider() {
      //it resolves to the non null entry (if any), otherwise it resolves to s1 (lower lexicographically)
      SiteEntryMode[] values = SiteEntryMode.values();
      Object[][] result = new Object[values.length * 2][3];
      int i = 0;
      for (SiteEntryMode mode : values) {
         result[i] = createInputs(mode, "LON", "NYC");
         result[i][2] = mode == SiteEntryMode.S1_NULL ? result[i][1] : result[i][0];
         ++i;
         result[i] = createInputs(mode, "1NYC", "2LON");
         result[i][2] = mode == SiteEntryMode.S1_NULL ? result[i][1] : result[i][0];
         ++i;
      }
      return result;
   }

   private static SiteEntry<String> newNullSiteEntry(String site) {
      return new SiteEntry<>(site, null, null);
   }

   private static SiteEntry<String> newSiteEntry(String site) {
      return new SiteEntry<>(site, TestingUtil.generateRandomString(8, RANDOM), null);
   }

   private static Object[] createInputs(SiteEntryMode mode, String lowerString, String upperString) {
      return new Object[]{mode.s1(lowerString), mode.s2(upperString), null};
   }

   @Test(dataProvider = "preferNonNullData")
   public void testPreferNonNullMergePolicy(SiteEntry<String> s1, SiteEntry<String> s2, SiteEntry<String> r) {
      doTest(preferNonNullMergePolicy(), s1, s2, r);
   }

   @Test(dataProvider = "preferNullData")
   public void testPreferNullMergePolicy(SiteEntry<String> s1, SiteEntry<String> s2, SiteEntry<String> r) {
      doTest(preferNullMergePolicy(), s1, s2, r);
   }

   @Test(dataProvider = "defaultData")
   public void testDefaultMergePolicy(SiteEntry<String> s1, SiteEntry<String> s2, SiteEntry<String> r) {
      doTest(defaultMergePolicy(), s1, s2, r);
   }

   @Test(dataProvider = "alwaysRemoveData")
   public void testAlwaysNull(SiteEntry<String> s1, SiteEntry<String> s2, SiteEntry<String> r) {
      doTest(alwaysRemoveMergePolicy(), s1, s2, r);
   }

   private void doTest(Sync mergePolicy, SiteEntry<String> s1, SiteEntry<String> s2, SiteEntry<String> expected) {
      SiteEntry<String> result = mergePolicy.merge(s1, s2);
      assertEquals(expected, result);
      result = mergePolicy.merge(s2, s1);
      assertEquals(expected, result);
   }

   private enum SiteEntryMode {
      BOTH_NON_NULL {
         @Override
         SiteEntry<String> s1(String site) {
            return newSiteEntry(site);
         }

         @Override
         SiteEntry<String> s2(String site) {
            return newSiteEntry(site);
         }
      },
      BOTH_NULL {
         @Override
         SiteEntry<String> s1(String site) {
            return newNullSiteEntry(site);
         }

         @Override
         SiteEntry<String> s2(String site) {
            return newNullSiteEntry(site);
         }
      },
      S1_NULL {
         @Override
         SiteEntry<String> s1(String site) {
            return newNullSiteEntry(site);
         }

         @Override
         SiteEntry<String> s2(String site) {
            return newSiteEntry(site);
         }
      },
      S2_NULL {
         @Override
         SiteEntry<String> s1(String site) {
            return newSiteEntry(site);
         }

         @Override
         SiteEntry<String> s2(String site) {
            return newNullSiteEntry(site);
         }
      };

      abstract SiteEntry<String> s1(String site);

      abstract SiteEntry<String> s2(String site);
   }

   private static class Sync {
      private final XSiteEntryMergePolicy<String, String> mergePolicy;


      private Sync(XSiteEntryMergePolicy<String, String> mergePolicy) {
         this.mergePolicy = mergePolicy;
      }

      SiteEntry<String> merge(SiteEntry<String> s1, SiteEntry<String> s2) {
         return CompletionStages.join(mergePolicy.merge("key", s1, s2));
      }
   }

}
