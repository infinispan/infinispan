/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.api.tree;

import org.infinispan.tree.Fqn;
import org.infinispan.tree.NodeKey;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.infinispan.util.concurrent.locks.containers.ReentrantStripedLockContainer;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * Tests the degree to which hash codes get spread
 */
@Test(groups = "unit", testName = "api.tree.TreeStructureHashCodeTest")
public class TreeStructureHashCodeTest {

   public void testHashCodesAppendedCount() {
      List<Fqn> fqns = new ArrayList<Fqn>();
      fqns.add(Fqn.ROOT);
      for (int i = 0; i < 256; i++) fqns.add(Fqn.fromString("/fqn" + i));
      doTest(fqns);
   }

   public void testHashCodesAlpha() {
      List<Fqn> fqns = new ArrayList<Fqn>();
      fqns.add(Fqn.ROOT);
      for (int i = 0; i < 256; i++) fqns.add(Fqn.fromString("/" + Integer.toString(i, 36)));
      doTest(fqns);
   }

   private void doTest(List<Fqn> fqns) {
      LockContainer container = new ReentrantStripedLockContainer(512);
      Map<Lock, Integer> distribution = new HashMap<Lock, Integer>();
      for (Fqn f : fqns) {
         NodeKey dataKey = new NodeKey(f, NodeKey.Type.DATA);
         NodeKey structureKey = new NodeKey(f, NodeKey.Type.STRUCTURE);
         addToDistribution(container.getLock(dataKey), distribution);
         addToDistribution(container.getLock(structureKey), distribution);
      }

      System.out.println("Distribution: " + distribution);
      assert distribution.size() <= container.size() : "Cannot have more locks than the container size!";
      // assume at least a 2/3rd even distribution
      // but also consider that data snd structure keys would typically provide the same hash code
      // so we need to double this
      assert distribution.size() * 1.5 * 2 >= container.size() : "Poorly distributed!  Distribution size is just " + distribution.size() + " and there are " + container.size() + " shared locks";

   }

   private void addToDistribution(Lock lock, Map<Lock, Integer> map) {
      int count = 1;
      if (map.containsKey(lock)) count = map.get(lock) + 1;
      map.put(lock, count);
   }
}
