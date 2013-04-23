/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.tasks;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "tasks.GlobalKeySetTaskTest")
public class GlobalKeySetTaskTest extends BaseDistFunctionalTest {

   public GlobalKeySetTaskTest() {
      sync = true;
      numOwners = 1;
      INIT_CLUSTER_SIZE = 2;
   }

   public void testGlobalKeySetTask() throws Exception {
      Cache<String,String> cache = cache(0);
      for (int i = 0; i < 1000; i++) {
         cache.put("k" + i, "v" + i);
      }

      Set<String> allKeys = GlobalKeySetTask.getGlobalKeySet(cache);

      assertEquals(1000, allKeys.size());
   }

}
