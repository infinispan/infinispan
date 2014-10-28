package org.infinispan.it.osgi;

import static org.infinispan.it.osgi.util.PaxExamUtils.exportTestPackages;
import static org.infinispan.it.osgi.util.PaxExamUtils.probeIsolationWorkaround;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Collection;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.it.osgi.util.IspnKarafOptions;
import org.infinispan.it.osgi.util.OSGiTestUtils;
import org.infinispan.manager.EmbeddedCacheManager;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.karaf.options.KarafDistributionOption;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
@Category(PerMethod.class)
public class InfinispanEmbeddedServiceFactoryTest {
   @org.ops4j.pax.exam.Configuration
   public Option[] config() throws Exception {
      return new Option[] {
            IspnKarafOptions.commonOptions(),
            /* The blueprint contained in this bundle will register a ManagedServiceFactory. */
            IspnKarafOptions.mvnFeature("org.infinispan", "infinispan-osgi", "infinispan-osgi"),
            KarafDistributionOption.replaceConfigurationFile("/etc/custom-etc-infinispan-config.xml", OSGiTestUtils.getResourceFile("org/infinispan/it/osgi/custom-config-3.xml"))
      };
   }

   @ProbeBuilder
   public TestProbeBuilder probe(TestProbeBuilder probeBuilder) {
      /* Export the test package (it contains the custom configuration files used in the tests). */
      return exportTestPackages(probeIsolationWorkaround(probeBuilder));
   }

   /**
    * If no configurations are present embedded cache manager instances are not created.
    * 
    * @throws Exception
    */
   @Test
   public void testNoConfiguration() throws Exception {
      BundleContext bundleContext = OSGiTestUtils.getBundleContext(this);

      ConfigurationAdmin configurationService = OSGiTestUtils.getService(bundleContext, ConfigurationAdmin.class);

      /* No pre-existing configurations are expected. */
      Configuration[] configurations = configurationService.listConfigurations("(service.pid=org.infinispan.manager.embedded)");
      assertNull("No configurations are expected.", configurations);

      Collection<ServiceReference<EmbeddedCacheManager>> serviceReferences;
      serviceReferences = bundleContext.getServiceReferences(EmbeddedCacheManager.class, "(service.pid=org.infinispan.manager.embedded)");
      assertEquals("No service is expected.", 0, serviceReferences.size());
   }

   /**
    * No services are created if the configuration doesn't contain the required properties.
    * 
    * @throws Exception
    */
   @Test
   public void testMissingConfigProperties() throws Exception {
      BundleContext bundleContext = OSGiTestUtils.getBundleContext(this);

      ConfigurationAdmin configurationService = OSGiTestUtils.getService(bundleContext, ConfigurationAdmin.class);

      /* No pre-existing configurations are expected. */
      Configuration[] configurations = configurationService.listConfigurations("(service.pid=org.infinispan.manager.embedded)");
      assertNull("No configurations are expected.", configurations);

      /* Create a new empty configuration for the service PID. */
      Configuration configuration = configurationService.createFactoryConfiguration("org.infinispan.manager.embedded", null);
      configuration.update();

      /* No services are expected as the 'config' parameter is not set. */
      Collection<ServiceReference<EmbeddedCacheManager>> serviceReferences;
      serviceReferences = bundleContext.getServiceReferences(EmbeddedCacheManager.class, "(service.pid=org.infinispan.manager.embedded)");

      assertNotNull("No service is expected.", serviceReferences);
      assertEquals("No service is expected.", 0, serviceReferences.size());
   }

   @Test
   public void testConfigurationPresent() throws Exception {
      BundleContext bundleContext = OSGiTestUtils.getBundleContext(this);

      final CountDownLatch expectedServiceRegistrations = new CountDownLatch(3);

      /* Add a listener which updates the service registration counter. */
      bundleContext.addServiceListener(new ServiceListener() {
         @Override
         public void serviceChanged(ServiceEvent event) {
            if (event.getType() == ServiceEvent.REGISTERED) {
               expectedServiceRegistrations.countDown();
            }
         }
      }, "(objectClass=org.infinispan.manager.EmbeddedCacheManager)");

      ConfigurationAdmin configurationService = OSGiTestUtils.getService(bundleContext, ConfigurationAdmin.class);

      /* No pre-existing configurations are expected. */
      Configuration[] configurations = configurationService.listConfigurations("(service.pid=org.infinispan.manager.embedded)");
      assertNull("No configurations are expected.", configurations);

      /* Create new empty configurations for the service PID. */
      Configuration configuration;
      Dictionary<String, Object> configProperties;

      /* First config. */
      configProperties = new Hashtable<String, Object>();
      configProperties.put("instanceId", "instance1");
      configProperties.put("config", "org/infinispan/it/osgi/custom-config-1.xml");
      configuration = configurationService.createFactoryConfiguration("org.infinispan.manager.embedded", null);
      configuration.update(configProperties);

      /* Second config. */
      configProperties = new Hashtable<String, Object>();
      configProperties.put("instanceId", "instance2");
      configProperties.put("config", "org/infinispan/it/osgi/custom-config-2.xml");
      configuration = configurationService.createFactoryConfiguration("org.infinispan.manager.embedded", null);
      configuration.update(configProperties);

      /* Third config is from the etc/ Karaf directory. */
      configProperties = new Hashtable<String, Object>();
      configProperties.put("instanceId", "instance3");
      configProperties.put("config", "etc/custom-etc-infinispan-config.xml");
      configuration = configurationService.createFactoryConfiguration("org.infinispan.manager.embedded", null);
      configuration.update(configProperties);

      /* Configuration updating and service registration are done concurrently. Await for then to complete first. */
      expectedServiceRegistrations.await(10, TimeUnit.SECONDS);

      Collection<ServiceReference<EmbeddedCacheManager>> serviceReferences;

      serviceReferences = bundleContext.getServiceReferences(EmbeddedCacheManager.class, "(instanceId=instance1)");
      assertEquals("Expecting the service to be registered through the mananged service factory.", 1, serviceReferences.size());

      serviceReferences = bundleContext.getServiceReferences(EmbeddedCacheManager.class, "(instanceId=instance2)");
      assertEquals("Expecting the service to be registered through the mananged service factory.", 1, serviceReferences.size());

      serviceReferences = bundleContext.getServiceReferences(EmbeddedCacheManager.class, "(instanceId=instance3)");
      assertEquals("Expecting the service to be registered through the mananged service factory.", 1, serviceReferences.size());
   }
}
