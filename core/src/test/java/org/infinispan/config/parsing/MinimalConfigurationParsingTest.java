/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.config.parsing;

import org.infinispan.config.InfinispanConfiguration;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.infinispan.test.TestingUtil.INFINISPAN_END_TAG;
import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG_NO_SCHEMA;

@Test(groups = "unit", testName = "config.MinimalConfigurationParsingTest")
public class MinimalConfigurationParsingTest {
   public void testGlobalAndDefaultSection() throws IOException {
      String xml = INFINISPAN_START_TAG +
              "    <global />\n" +
              "    <default>\n" +
            "        <locking concurrencyLevel=\"10000\" isolationLevel=\"READ_COMMITTED\" />\n" +
              "    </default>\n" +
              INFINISPAN_END_TAG;
      testXml(xml);
   }

   public void testNoGlobalSection() throws IOException {
      String xml = INFINISPAN_START_TAG +
              "    <default>\n" +
            "        <locking concurrencyLevel=\"10000\" isolationLevel=\"READ_COMMITTED\" />\n" +
              "    </default>\n" +
              INFINISPAN_END_TAG;
      testXml(xml);
   }

   public void testNoDefaultSection() throws IOException {
      String xml = INFINISPAN_START_TAG +
            "    <global />\n" +
            INFINISPAN_END_TAG;
      testXml(xml);
   }

   public void testNoSections() throws IOException {
      String xml = INFINISPAN_START_TAG + INFINISPAN_END_TAG;
      testXml(xml);
   }

   public void testNoSchema() throws IOException {
      String xml = INFINISPAN_START_TAG_NO_SCHEMA + INFINISPAN_END_TAG;
      testXml(xml);
   }

   private void testXml(String xml) throws IOException {
      InputStream stream = new ByteArrayInputStream(xml.getBytes());
      InfinispanConfiguration ic = InfinispanConfiguration.newInfinispanConfiguration(stream);
      assert ic != null;
   }
}
