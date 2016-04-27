package org.infinispan.xsite;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

/**
 * Abstract test class that allows to create multiple combination of sites and nodes.
 * <p>
 * So far, each site only has the same number of nodes. But it can modified to support asymmetric number of nodes per
 * site.
 *
 * @author Pedro Ruivo
 * @since 8.1
 */
public abstract class AbstractMultipleSitesTest extends AbstractXSiteTest {

   //if more sites are needed, you have to update these constants and the configuration file in config/xsite/relay-config.xml
   //the SITE_NAME should have the same site name as in configuration
   private static final int MAX_NUM_SITE = 3;
   private static final String[] SITE_NAME = {"LON", "NYC", "SFO"};

   /**
    * It returns the number of sites to create.
    * <p>
    * It may be overwrite for different number of sites needed. The default value is 2 and it the value should be less
    * than {@link #MAX_NUM_SITE}.
    */
   protected int defaultNumberOfSites() {
      return 2;
   }

   /**
    * @return the number of nodes per site.
    */
   protected int defaultNumberOfNodes() {
      return 2;
   }

   /**
    * The default cache configuration for that site index.
    *
    * @param siteIndex the site index.
    */
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      return new ConfigurationBuilder();
   }

   /**
    * The default global configuration for a site.
    *
    * @param siteIndex the site index.
    */
   protected GlobalConfigurationBuilder defaultGlobalConfigurationForSite(int siteIndex) {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      builder.site().localSite(siteName(siteIndex));
      return builder;
   }

   /**
    * It converts the site index to a name.
    *
    * @param siteIndex the site index to be converted.
    * @return the site name corresponding to the site index.
    */
   protected final String siteName(int siteIndex) {
      assertValidSiteIndex(siteIndex);
      return SITE_NAME[siteIndex];
   }

   /**
    * Invoked after all the sites and default caches are created.
    */
   protected void afterSitesCreated() {

   }

   @Override
   protected void createSites() {
      final int numberOfSites = defaultNumberOfSites();
      if (numberOfSites <= 0) {
         throw new IllegalArgumentException("Default number of sites must be positive.");
      } else if (numberOfSites > MAX_NUM_SITE) {
         throw new IllegalArgumentException("Default number of sites must be less than the max number of configured sites.");
      }
      for (int siteIndex = 0; siteIndex < defaultNumberOfSites(); siteIndex++) {
         createSite(siteName(siteIndex), defaultNumberOfNodes(), defaultGlobalConfigurationForSite(siteIndex), defaultConfigurationForSite(siteIndex));
      }
      afterSitesCreated();
   }

   private void assertValidSiteIndex(int index) {
      if (index < 0) {
         throw new IllegalArgumentException("Site index must be positive or zero.");
      } else if (index >= MAX_NUM_SITE) {
         throw new IllegalArgumentException("Site index must be less than the max number of configured sites.");
      } else if (index >= defaultNumberOfSites()) {
         throw new IllegalArgumentException("Site index must be less than the number of sites configured.");
      }
   }

}
