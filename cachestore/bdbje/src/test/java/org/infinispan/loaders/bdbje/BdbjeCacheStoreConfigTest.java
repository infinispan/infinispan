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
package org.infinispan.loaders.bdbje;

import org.infinispan.loaders.CacheLoaderException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests that cover {@link  BdbjeCacheStoreConfig }
 *
 * @author Adrian Cole
 * @since 4.0
 */
@Test(groups = "unit", enabled = true, testName = "loaders.bdbje.BdbjeCacheStoreConfigTest")
public class BdbjeCacheStoreConfigTest {

    private BdbjeCacheStoreConfig config;

    @BeforeMethod
    public void setUp() throws Exception {
        config = new BdbjeCacheStoreConfig();
    }

    @AfterMethod
    public void tearDown() throws CacheLoaderException {
        config = null;
    }


    @Test
    public void testGetClassNameDefault() {
        assert config.getCacheLoaderClassName().equals(BdbjeCacheStore.class.getName());
    }

    @Test
    public void testGetMaxTxRetries() {
        assert config.getMaxTxRetries() == 5;
    }

    @Test
    public void testSetMaxTxRetries() {
        config.setMaxTxRetries(1);
        assert config.getMaxTxRetries() == 1;
    }

    @Test
    public void testGetLockAcquistionTimeout() {
        assert config.getLockAcquistionTimeout() == 60 * 1000;
    }

    @Test
    public void testSetLockAcquistionTimeoutMicros() {
        config.setLockAcquistionTimeout(1);
        assert config.getLockAcquistionTimeout() == 1;
    }

    @Test
    public void testGetLocationDefault() {
        assert config.getLocation().equals("Infinispan-BdbjeCacheStore");
    }

    @Test
    public void testSetLocation() {
        config.setLocation("foo");
        assert config.getLocation().equals("foo");
    }

    @Test
    public void testSetCacheDb() {
        config.setCacheDbNamePrefix("foo");
        assert config.getCacheDbNamePrefix().equals("foo");
    }

    @Test
    public void testSetCatalogDb() {
        config.setCatalogDbName("foo");
        assert config.getCatalogDbName().equals("foo");
    }

    @Test
    public void testSetExpiryDb() {
        config.setExpiryDbNamePrefix("foo");
        assert config.getExpiryDbPrefix().equals("foo");
    }

}
