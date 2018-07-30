package org.infinispan.it.osgi.hibernate.cache.v53;

import javax.inject.Inject;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.infinispan.it.osgi.util.CustomPaxExamRunner;
import org.infinispan.it.osgi.util.MavenUtils;
import org.infinispan.it.osgi.util.PaxExamUtils;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;

import org.apache.karaf.features.FeaturesService;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

import static org.infinispan.it.osgi.util.IspnKarafOptions.bundlePaxExamSpi;
import static org.infinispan.it.osgi.util.IspnKarafOptions.karafContainer;
import static org.infinispan.it.osgi.util.IspnKarafOptions.mvnFeature;
import static org.infinispan.it.osgi.util.IspnKarafOptions.runWithoutConsole;
import static org.infinispan.it.osgi.util.IspnKarafOptions.verboseKaraf;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.maven;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;

/**
 * Tests infinispan-hibernate-cache-v53 feature,
 * running within a Karaf container via PaxExam.
 *
 * @author Fabio Massimo Ercoli
 */
@RunWith(CustomPaxExamRunner.class)
@ExamReactorStrategy(PerClass.class)
@Category(PerClass.class)
public class HibernateCacheIntegrationTest {

   private static final String VERSION_HIBERNATE_OSGI = "version.hibernate.osgi";

   @Configuration
   public Option[] config() throws Exception {
      String hibernate53Version = MavenUtils.getProperties().getProperty(VERSION_HIBERNATE_OSGI);

      return options(
         karafContainer(),
         verboseKaraf(),
         runWithoutConsole(),
         keepRuntimeFolder(),
         bundlePaxExamSpi(),
         mvnFeature("org.infinispan", "infinispan-hibernate-cache-v53", "infinispan-hibernate-cache-v53"),
         features(maven().groupId("org.hibernate").artifactId("hibernate-osgi").type("xml")
            .classifier("karaf").version(hibernate53Version), "hibernate-orm"));
   }

   @ProbeBuilder
   public TestProbeBuilder builder(TestProbeBuilder probeBuilder) {
      return PaxExamUtils.probeIsolationWorkaround(probeBuilder);
   }

   @Inject
   private FeaturesService featuresService;

   @Inject
   private BundleContext bundleContext;

   @Test public void testActivation() throws Exception {
      assertTrue(featuresService.isInstalled(featuresService.getFeature("hibernate-orm")));
      assertTrue(featuresService.isInstalled(featuresService.getFeature("infinispan-core")));
      assertTrue(featuresService.isInstalled(featuresService.getFeature("infinispan-hibernate-cache-v53")));

      assertActiveBundle("org.hibernate.orm.core");
      assertActiveBundle("org.infinispan.core");
   }

   private void assertActiveBundle(String symbolicName) {
      for (Bundle bundle : bundleContext.getBundles()) {
         if (bundle.getSymbolicName().equals(symbolicName)) {
            Assert.assertEquals(symbolicName + " was found, but not in an ACTIVE state.", Bundle.ACTIVE, bundle.getState());
            return;
         }
      }
      Assert.fail("Could not find bundle: " + symbolicName);
   }

}
