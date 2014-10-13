package org.infinispan.it.osgi.features;

import static org.infinispan.it.osgi.util.IspnKarafOptions.commonOptions;
import static org.ops4j.pax.exam.CoreOptions.options;

import java.net.URI;
import java.util.Properties;

import org.apache.karaf.features.FeaturesService;
import org.infinispan.it.osgi.util.MavenUtils;
import org.infinispan.it.osgi.util.PaxExamUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;

/**
 * Tests features.xml definitions for OSGi bundles.
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
@Category(PerClass.class)
public class OSGiKarafFeaturesTest {
   private static final String PROP_PROJECT_VERSION = "project.version";

   @Configuration
   public Option[] config() throws Exception {
      return options(commonOptions());
   }

   @ProbeBuilder
   public TestProbeBuilder builder(TestProbeBuilder probeBuilder) {
      return PaxExamUtils.probeIsolationWorkaround(probeBuilder);
   }

   /**
    * Verifies that Karaf Features install correctly on clean containers.
    */
   @Test
   public void testCleanInstall() throws Exception {
      Bundle bundle = FrameworkUtil.getBundle(getClass());
      Assert.assertNotNull("Failed to find class bundle.", bundle);

      BundleContext context = bundle.getBundleContext();
      ServiceReference<FeaturesService> serviceReference = context.getServiceReference(FeaturesService.class);
      Assert.assertNotNull("Failed to obtain a reference to the FeaturesService.", serviceReference);

      FeaturesService service = context.getService(serviceReference);
      Assert.assertNotNull("Failed to obtain a FeaturesService instance.", service);

      Properties mavenProps = MavenUtils.getProperties();
      String version = mavenProps.getProperty(PROP_PROJECT_VERSION);
      Assert.assertNotNull("Failed to obtain the project version from maven.", version);

      checkInstall(service, "infinispan-commons", "infinispan-commons", version);
      checkInstall(service, "infinispan-core", "infinispan-core", version);
      checkInstall(service, "infinispan-client-hotrod", /* deprecated */ "hotrod-client", version);
      checkInstall(service, "infinispan-client-hotrod", /* deprecated */ "hotrod-client-with-query", version);
      checkInstall(service, "infinispan-client-hotrod", "infinispan-client-hotrod", version);
      checkInstall(service, "infinispan-client-hotrod", "infinispan-client-hotrod-with-query", version);
      checkInstall(service, "infinispan-cachestore-jdbc", "infinispan-cachestore-jdbc", version);
      checkInstall(service, "infinispan-cachestore-remote", "infinispan-cachestore-remote", version);
      checkInstall(service, "infinispan-cachestore-leveldb", "infinispan-cachestore-leveldb-jni", version);
      checkInstall(service, "infinispan-cachestore-leveldb", "infinispan-cachestore-leveldb-java", version);
      checkInstall(service, "infinispan-cachestore-jpa", "infinispan-cachestore-jpa", version);
      checkInstall(service, "infinispan-osgi", "infinispan-osgi", version);
   }

   private void checkInstall(FeaturesService service, String artifactId, String feature, String version) throws Exception {
      Assert.assertNull(String.format("Feature '%s' version '%s' is present in the container before install!", feature, version),
            service.getFeature(feature, version));

      try {
         service.installFeature(feature, version);
         Assert.fail("Feature install should fail before the repository is added.");
      } catch (Exception ex) {
      }

      URI repoUri = new URI(String.format("mvn:org.infinispan/%s/%s/xml/features", artifactId, version));
      service.addRepository(repoUri);

      service.installFeature(feature, version);
      Assert.assertNotNull(String.format("Feature '%s' version '%s' install failed.", feature, version),
            service.getFeature(feature, version));

      /* Clean-up. */
      service.uninstallFeature(feature, version);
      service.removeRepository(repoUri);

      Assert.assertNull(String.format("Feature '%s' version '%s' is still present in the container after uninstall!", feature, version),
            service.getFeature(feature, version));
   }
}
