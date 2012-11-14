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

package org.infinispan.distribution;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "distribution.InvalidationNoReplicationNoTxTest")
public class InvalidationNoReplicationNoTxTest extends InvalidationNoReplicationTest {

   public InvalidationNoReplicationNoTxTest() {
      transactional = false;
   }

   public void testInvalidation() throws Exception {
      cache(1).put(k0, "v0");
      assert advancedCache(0).getDataContainer().containsKey(k0);
      assert !advancedCache(1).getDataContainer().containsKey(k0);

      assertEquals(cache(1).get(k0), "v0");
      assert advancedCache(0).getDataContainer().containsKey(k0);
      assert advancedCache(1).getDataContainer().containsKey(k0);

      log.info("Here is the put!");
      log.infof("Cache 0=%s cache 1=%s", address(0), address(1));
      cache(0).put(k0, "v1");

      log.info("before assertions!");
      assertEquals(advancedCache(1).getDataContainer().get(k0), null);
      assertEquals(advancedCache(0).getDataContainer().get(k0).getValue(), "v1");
   }

}
