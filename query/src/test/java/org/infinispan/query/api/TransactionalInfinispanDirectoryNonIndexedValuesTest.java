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
package org.infinispan.query.api;

import org.testng.annotations.Test;

/**
 * Testing non-indexed values on InfinispanDirectory in case of transactional cache.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.api.TransactionalInfinispanDirectoryNonIndexedValuesTest", enabled = false,
      description = "Enable when the ISPN-2815 is fixed.")
public class TransactionalInfinispanDirectoryNonIndexedValuesTest extends InfinispanDirectoryNonIndexedValuesTest {

   protected boolean isTransactional() {
      return true;
   }

   @Test(enabled = false, description = "Enable when the ISPN-2815 is fixed.")
   public void testReplaceSimpleSearchable() {

   }
}
