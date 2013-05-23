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
package org.infinispan.distribution;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.replication.SyncReplPessimisticLockingTest;

import org.testng.annotations.Test;

/**
 * Tests for implicit locking
 * <p/>
 * Transparent eager locking for transactions https://jira.jboss.org/jira/browse/ISPN-70
 *
 * @author Vladimir Blagojevic
 */
@Test(groups = "functional", testName = "distribution.SyncDistPessimisticLockingTest")
public class SyncDistPessimisticLockingTest extends SyncReplPessimisticLockingTest {

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }
 }
