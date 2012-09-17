/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.xsite;

import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.SiteConfiguration;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
@Test(groups = "functional, xsite", testName = "xsite.XSiteGlobalConfigurationTest")
public class XSiteGlobalConfigurationTest {

   public void testBasicCreation() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      GlobalConfiguration gConfig = gcb
            .sites().localSite("LON")
            .addSite().name("LON")
            .sites()
            .addSite().name("SFO")
            .sites()
            .addSite().name("NYC").build();

      assertEquals(gConfig.sites().localSite(), "LON");
      List<SiteConfiguration> siteConfigurations = gConfig.sites().siteConfigurations();
      assertEquals(siteConfigurations.size(), 3);
      assertEquals(siteConfigurations.get(0).name(), "LON");
      assertEquals(siteConfigurations.get(1).name(), "SFO");
      assertEquals(siteConfigurations.get(2).name(), "NYC");
   }

   @Test(expectedExceptions = ConfigurationException.class)
   public void testLocalSiteNameNotSpecified() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb
            .sites()
            .addSite().name("LON")
            .sites()
            .addSite().name("SFO")
            .sites()
            .addSite().name("NYC");
      gcb.build();
   }

   @Test(expectedExceptions = ConfigurationException.class)
   public void testLocalSiteIncorrect() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb
            .sites().localSite("NO_SUCH_SITE")
            .addSite().name("LON")
            .sites()
            .addSite().name("SFO")
            .sites()
            .addSite().name("NYC");
      gcb.build();
   }

   @Test(expectedExceptions = ConfigurationException.class)
   public void testSiteNameNotSpecified() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb
            .sites().localSite("LON")
            .addSite().name("LON")
            .sites()
            .addSite().name("SFO")
            .sites()
            .addSite();
      gcb.build();
   }

   public void testNoSitesDefined() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.sites();
      gcb.build(); //should not throw any exception
   }
}
