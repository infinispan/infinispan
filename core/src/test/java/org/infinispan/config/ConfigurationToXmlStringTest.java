package org.infinispan.config;

import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.StringReader;

import static org.infinispan.config.InfinispanConfiguration.*;
import static org.testng.Assert.assertTrue;

/**
 * Tests that InfinispanConfiguration, GlobalConfiguration and Configuration can be encoded to an XML string.
 *
 * @author Juergen_Kellerer, 2011-03-13
 */
@Test(groups = "unit", testName = "config.ConfigurationToXmlStringTest")
public class ConfigurationToXmlStringTest {

   @Test
   public void testCanMarshalInfinispanConfigurationToXml() throws Exception {
      InfinispanConfiguration configuration = newInfinispanConfiguration(
          getClass().getResourceAsStream("/configs/named-cache-test.xml"));
      configuration.parseGlobalConfiguration().fluent().transport().clusterName("MyCluster");

      assertXmlStringContains(configuration.toXmlString(), "clusterName=\"MyCluster\"");
   }

   @Test
   public void testCanMarshalGlobalConfigurationToXml() throws Exception {
      GlobalConfiguration configuration = new GlobalConfiguration();
      configuration.fluent().transport().clusterName("MyCluster").siteId("MySite");
      configuration.fluent().globalJmxStatistics();

      assertXmlStringContains(configuration.toXmlString(),
          "clusterName=\"MyCluster\"", "siteId=\"MySite\"", "enabled=\"true\"");
   }

   @Test
   public void testCanMarshalConfigurationToXml() throws Exception {
      Configuration configuration = new Configuration();
      configuration.name = "MyCacheName";
      configuration.fluent().eviction().maxEntries(10).strategy(EvictionStrategy.LIRS);
      configuration.fluent().locking().concurrencyLevel(123).isolationLevel(IsolationLevel.NONE);

      assertXmlStringContains(configuration.toXmlString(), "name=\"MyCacheName\"",
          "maxEntries=\"10\"", "strategy=\"LIRS\"",
          "concurrencyLevel=\"123\"", "isolationLevel=\"NONE\"");
   }

   void assertXmlStringContains(String xmlString, String... containedFragments) {
      xmlString = parseAndEncode(xmlString);
      for (String fragment : containedFragments)
         assertTrue(xmlString.contains(fragment));
   }

   String parseAndEncode(String xmlString) {
      try {
         Object instance = getJAXBContext().createUnmarshaller().unmarshal(new StringReader(xmlString));
         return toXmlString(instance);
      } catch (JAXBException e) {
         throw new RuntimeException(e);
      }
   }
}
