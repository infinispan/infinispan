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
package org.infinispan.distribution;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "distribution.MagicKeyTest")
public class MagicKeyTest extends BaseDistFunctionalTest {
   public void testMagicKeys() {
      MagicKey k1 = new MagicKey(c1);
      assert getDistributionManager(c1).getLocality(k1).isLocal();
      assert getDistributionManager(c2).getLocality(k1).isLocal();
      assert !getDistributionManager(c3).getLocality(k1).isLocal();
      assert !getDistributionManager(c4).getLocality(k1).isLocal();

      MagicKey k2 = new MagicKey(c2);
      assert !getDistributionManager(c1).getLocality(k2).isLocal();
      assert getDistributionManager(c2).getLocality(k2).isLocal();
      assert getDistributionManager(c3).getLocality(k2).isLocal();
      assert !getDistributionManager(c4).getLocality(k2).isLocal();

      MagicKey k3 = new MagicKey(c3);
      assert !getDistributionManager(c1).getLocality(k3).isLocal();
      assert !getDistributionManager(c2).getLocality(k3).isLocal();
      assert getDistributionManager(c3).getLocality(k3).isLocal();
      assert getDistributionManager(c4).getLocality(k3).isLocal();

      MagicKey k4 = new MagicKey(c4);
      assert getDistributionManager(c1).getLocality(k4).isLocal();
      assert !getDistributionManager(c2).getLocality(k4).isLocal();
      assert !getDistributionManager(c3).getLocality(k4).isLocal();
      assert getDistributionManager(c4).getLocality(k4).isLocal();
   }
}
