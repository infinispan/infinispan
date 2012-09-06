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
package org.infinispan.configuration;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "configuration.HashConfigurationBuilderTest")
public class HashConfigurationBuilderTest {
    
    @Test
    public void testNumOwners(){
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.clustering().cacheMode(CacheMode.DIST_SYNC);
        cb.clustering().hash().numOwners(5);
        
        Configuration c = cb.build();
        Assert.assertEquals(5, c.clustering().hash().numOwners());
        
        try {
            cb.clustering().hash().numOwners(0);
            Assert.fail("IllegalArgumentException expected");
        } catch(IllegalArgumentException e){
        }
    }

    @Test
    public void testNumSegments(){
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.clustering().cacheMode(CacheMode.DIST_SYNC);
        cb.clustering().hash().numSegments(5);

        Configuration c = cb.build();
        Assert.assertEquals(5, c.clustering().hash().numSegments());

        try {
            cb.clustering().hash().numSegments(0);
            Assert.fail("IllegalArgumentException expected");
        } catch(IllegalArgumentException e){
        }
    }
}
