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
package org.infinispan.distribution.topologyaware;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.distribution.ch.TopologyAwareSyncConsistentHashFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "topologyaware.TopologyAwareConsistentHashFactoryTest")
public class TopologyAwareSyncConsistentHashFactoryTest extends TopologyAwareConsistentHashFactoryTest {

   public TopologyAwareSyncConsistentHashFactoryTest() {
      // Increase the number of segments to eliminate collisions (which would cause extra segment movements,
      // causing testConsistencyAfterLeave to fail.)
      numSegments = 1000;
   }

   @Override
   protected ConsistentHashFactory<DefaultConsistentHash> createConsistentHashFactory() {
      return new TopologyAwareSyncConsistentHashFactory();
   }
}
