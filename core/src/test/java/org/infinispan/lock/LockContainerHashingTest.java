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
package org.infinispan.lock;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.locks.containers.AbstractStripedLockContainer;
import org.infinispan.util.concurrent.locks.containers.ReentrantStripedLockContainer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;

@Test(groups = "unit", testName = "lock.LockContainerHashingTest")
public class LockContainerHashingTest extends AbstractInfinispanTest {
   private AbstractStripedLockContainer stripedLock;

   @BeforeMethod
   public void setUp() {
      stripedLock = new ReentrantStripedLockContainer(500);
   }

   public void testHashingDistribution() {
      // ensure even bucket distribution of lock stripes
      List<String> keys = createRandomKeys(1000);

      Map<Lock, Integer> distribution = new HashMap<Lock, Integer>();

      for (String s : keys) {
         Lock lock = stripedLock.getLock(s);
         if (distribution.containsKey(lock)) {
            int count = distribution.get(lock) + 1;
            distribution.put(lock, count);
         } else {
            distribution.put(lock, 1);
         }
      }

      System.out.println(distribution);

      // cannot be larger than the number of locks
      log.trace("dist size: " + distribution.size());
      log.trace("num shared locks: " + stripedLock.size());
      assert distribution.size() <= stripedLock.size();
      // assume at least a 2/3rd spread
      assert distribution.size() * 1.5 >= stripedLock.size();
   }

   private List<String> createRandomKeys(int number) {

      List<String> f = new ArrayList<String>(number);
      Random r = new Random();
      int i = number;
      while (f.size() < number) {
         String s = i + "baseKey" + (10000 + i++);
         f.add(s);
      }

      return f;
   }
}
