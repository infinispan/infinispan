package org.infinispan.configuration;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.DefaultDataContainer;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import static org.infinispan.test.AbstractInfinispanTest.TIME_SERVICE;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

@Test(testName = "config.DataContainerTest", groups = "functional")
public class DataContainerTest {

   @Test
   public void testDefault() throws IOException {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
              "<infinispan>" +
              "<cache-container name=\"1\" default-cache=\"default-cache\">" +
              "<local-cache name=\"default-cache\" />" +
              "</cache-container>" +
              "</infinispan>";

      InputStream stream = new ByteArrayInputStream(xml.getBytes());
      EmbeddedCacheManager cm = TestCacheManagerFactory.fromStream(stream);

      try
      {
         // Verify that the configuration is correct
         Assert.assertNull(cm.getDefaultCacheConfiguration().dataContainer().dataContainer());

         Assert.assertEquals(cm.getCache().getAdvancedCache().getDataContainer().getClass(), DefaultDataContainer.class);
      }
      finally
      {
      	TestingUtil.killCacheManagers(cm);
      }
   }

   @Test
   public void testCustomDataContainerClass() throws IOException {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<infinispan>" +
            "<cache-container name=\"1\" default-cache=\"default-cache\">" +
            "<local-cache name=\"default-cache\">" +
            "  <data-container class=\"org.infinispan.configuration.QueryableDataContainer\">" +
            "     <property name=\"foo\">bar</property>" +
            "  </data-container>" +
            "</local-cache>" +
            "</cache-container>" +
            "</infinispan>";

      InputStream stream = new ByteArrayInputStream(xml.getBytes());
      EmbeddedCacheManager cm = TestCacheManagerFactory.fromStream(stream);
      try {
         AdvancedCache<Object, Object> cache = cm.getCache().getAdvancedCache();

         DataContainer ddc = DefaultDataContainer.unBoundedDataContainer(cache.getCacheConfiguration().locking().concurrencyLevel());
         ActivationManager activationManager = mock(ActivationManager.class);
         doNothing().when(activationManager).onUpdate(Mockito.anyObject(), Mockito.anyBoolean());
         ((DefaultDataContainer) ddc).initialize(null, null,new InternalEntryFactoryImpl(), activationManager, null, TIME_SERVICE);
         QueryableDataContainer.setDelegate(ddc);

         // Verify that the default is correctly established
         Assert.assertEquals(cm.getDefaultCacheConfiguration().dataContainer().dataContainer().getClass().getName(), QueryableDataContainer.class.getName());

         Assert.assertEquals(cache.getDataContainer().getClass(), QueryableDataContainer.class);

         QueryableDataContainer dataContainer = QueryableDataContainer.class.cast(cache.getDataContainer());

         Assert.assertFalse(checkLoggedOperations(dataContainer.getLoggedOperations(), "setFoo(bar)"));

         cache.put("name", "Pete");

         Assert.assertTrue(checkLoggedOperations(dataContainer.getLoggedOperations(), "put(name, Pete"));
      } finally {
      	TestingUtil.killCacheManagers(cm);
      }

   }

   @Test
   public void testCustomDataContainer() {

   	ConfigurationBuilder configuration = new ConfigurationBuilder();
   	configuration.dataContainer().dataContainer(new QueryableDataContainer());

      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(configuration);

      try {
         AdvancedCache<Object, Object> cache = cm.getCache().getAdvancedCache();

         DataContainer ddc = DefaultDataContainer.unBoundedDataContainer(cache.getCacheConfiguration().locking().concurrencyLevel());
         ActivationManager activationManager = mock(ActivationManager.class);
         doNothing().when(activationManager).onUpdate(Mockito.anyObject(), Mockito.anyBoolean());
         ((DefaultDataContainer) ddc).initialize(null, null,new InternalEntryFactoryImpl(), activationManager, null, TIME_SERVICE);
         QueryableDataContainer.setDelegate(ddc);

         // Verify that the config is correct
         Assert.assertEquals(cm.getDefaultCacheConfiguration().dataContainer().dataContainer().getClass(), QueryableDataContainer.class);

         Assert.assertEquals(cache.getDataContainer().getClass(), QueryableDataContainer.class);

         QueryableDataContainer dataContainer = QueryableDataContainer.class.cast(cache.getDataContainer());

         cache.put("name", "Pete");

         Assert.assertTrue(checkLoggedOperations(dataContainer.getLoggedOperations(), "put(name, Pete"));
      } finally {
      	TestingUtil.killCacheManagers(cm);
      }
   }

   boolean checkLoggedOperations(Collection<String> loggedOperations, String prefix) {
      for (String loggedOperation : loggedOperations) {
         if (loggedOperation.startsWith(prefix)) {
            return true;
         }
      }
      return false;
   }

}
