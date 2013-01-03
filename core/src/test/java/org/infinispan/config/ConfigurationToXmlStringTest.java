/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

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
