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
package org.infinispan.loaders.hbase;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.keymappers.MarshalledValueOrPrimitiveMapper;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.hbase.HBaseCacheStoreConfigTest")
public class HBaseCacheStoreConfigTest {

   public void setConfigurationPropertiesFileTest() throws CacheLoaderException {
      HBaseCacheStoreConfig config = new HBaseCacheStoreConfig();

      assert config.entryTable == "ISPNCacheStore";
      assert config.hbaseZookeeperQuorum == "localhost";
      assert config.hbaseZookeeperPropertyClientPort == 2181;
      assert config.entryColumnFamily == "E";
      assert config.entryValueField == "EV";
      assert config.expirationTable == "ISPNCacheStoreExpiration";
      assert config.expirationColumnFamily == "X";
      assert config.expirationValueField == "XV";
      assert config.autoCreateTable;
      assert !config.sharedTable;
      assert config.keyMapper == MarshalledValueOrPrimitiveMapper.class.getName();
   }

}
