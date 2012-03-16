/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.client.hotrod;

import org.testng.annotations.Test;

import java.util.Properties;

/**
 * Tests consistent hash algorithm consistency between the client and server
 * using Hot Rod's 1.0 protocol.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "client.hotrod.HotRod10CSAIntegrationTest")
public class HotRod10CSAIntegrationTest extends CSAIntegrationTest {

   @Override
   protected void setHotRodProtocolVersion(Properties props) {
      props.setProperty("infinispan.client.hotrod.protocol_version", "1.0");
   }

}
