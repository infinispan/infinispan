package org.infinispan.config;

import org.infinispan.AdvancedCache;
import org.infinispan.container.DataContainer;
import org.infinispan.container.DefaultDataContainer;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import static org.infinispan.test.AbstractInfinispanTest.TIME_SERVICE;

@Test(testName = "config.DataContainerTest", groups = "functional")
public class DataContainerTest {

   @Test
   public void testDefault() throws IOException {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
              "<infinispan>" +
              "<default><dataContainer /></default>" + 
              "</infinispan>";

      InputStream stream = new ByteArrayInputStream(xml.getBytes());
      EmbeddedCacheManager cm = TestCacheManagerFactory.fromStream(stream);
      
      try
      {
         // Verify that the configuration is correct
         Assert.assertEquals(cm.getDefaultConfiguration().getDataContainerClass(), DefaultDataContainer.class.getName());
         
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
              "<default><dataContainer class=\"" + QueryableDataContainer.class.getName() + "\">" +
              "<properties><property name=\"foo\" value=\"bar\" /></properties>" +
           	  "</dataContainer></default>" + 
              "</infinispan>";

      InputStream stream = new ByteArrayInputStream(xml.getBytes());
      EmbeddedCacheManager cm = TestCacheManagerFactory.fromStream(stream);
      try {
         AdvancedCache<Object, Object> cache = cm.getCache().getAdvancedCache();

         DataContainer ddc = DefaultDataContainer.unBoundedDataContainer(cache.getConfiguration().getConcurrencyLevel());
         ((DefaultDataContainer) ddc).initialize(null, null,new InternalEntryFactoryImpl(), null, null, TIME_SERVICE);
         QueryableDataContainer.setDelegate(ddc);

         // Verify that the default is correctly established
         Assert.assertEquals(cm.getDefaultConfiguration().getDataContainer().getClass().getName(), QueryableDataContainer.class.getName());
         
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

   	Configuration configuration = new Configuration();
   	configuration.fluent().dataContainer().dataContainer(new QueryableDataContainer());
   	
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(configuration);
      
      try {
         AdvancedCache<Object, Object> cache = cm.getCache().getAdvancedCache();

         DataContainer ddc = DefaultDataContainer.unBoundedDataContainer(cache.getConfiguration().getConcurrencyLevel());
         ((DefaultDataContainer) ddc).initialize(null, null,new InternalEntryFactoryImpl(), null, null, TIME_SERVICE);
         QueryableDataContainer.setDelegate(ddc);

         // Verify that the config is correct
         Assert.assertEquals(cm.getDefaultConfiguration().getDataContainer().getClass(), QueryableDataContainer.class);
         
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
   		if (loggedOperation.startsWith(prefix))
   		{
   			return true;
   		}
      }
   	return false;
   }
    
}
