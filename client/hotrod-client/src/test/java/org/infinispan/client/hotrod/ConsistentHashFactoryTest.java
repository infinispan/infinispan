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
package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;

/**
 * Tester for ConsistentHashFactory.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.ConsistentHashFactoryTest", groups = "functional")
public class ConsistentHashFactoryTest {

   public void testPropertyCorrectlyRead() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.consistentHashImpl(1, SomeCustomConsistentHashV1.class);
      ConsistentHashFactory chf = new ConsistentHashFactory();
      chf.init(builder.build());
      ConsistentHash hash = chf.newConsistentHash(1);
      assertNotNull(hash);
      assertEquals(hash.getClass(), SomeCustomConsistentHashV1.class);
   }

   public void testNoChDefined() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      ConsistentHashFactory chf = new ConsistentHashFactory();
      chf.init(builder.build());
      ConsistentHash hash = chf.newConsistentHash(1);
      assertNotNull(hash);
      assertEquals(hash.getClass(), ConsistentHashV1.class);
   }
}
