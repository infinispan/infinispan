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
package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "jmx.RpcManagerMBeanTest")
public class RpcManagerMBeanTest extends MultipleCacheManagersTest {
   private final String cachename = "repl_sync_cache";
   public static final String JMX_DOMAIN = RpcManagerMBeanTest.class.getSimpleName();
   public static final String JMX_DOMAIN2 = JMX_DOMAIN + "2";

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setAllowDuplicateDomains(true);
      globalConfiguration.setJmxDomain(JMX_DOMAIN);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      CacheContainer cacheManager1 = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfiguration);
      cacheManager1.start();

      GlobalConfiguration globalConfiguration2 = GlobalConfiguration.getClusteredDefault();
      globalConfiguration2.setExposeGlobalJmxStatistics(true);
      globalConfiguration2.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration2.setJmxDomain(JMX_DOMAIN);
      globalConfiguration2.setAllowDuplicateDomains(true);
      CacheContainer cacheManager2 = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfiguration2);
      cacheManager2.start();

      registerCacheManager(cacheManager1, cacheManager2);

      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      config.setExposeJmxStatistics(true);
      defineConfigurationOnAllManagers(cachename, config);
   }

   public void testEnableJmxStats() throws Exception {
      Cache<String, String> cache1 = manager(0).getCache(cachename);
      Cache cache2 = manager(1).getCache(cachename);
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName rpcManager1 = getCacheObjectName(JMX_DOMAIN, cachename + "(repl_sync)", "RpcManager");
      ObjectName rpcManager2 = getCacheObjectName(JMX_DOMAIN2, cachename + "(repl_sync)", "RpcManager");
      assert mBeanServer.isRegistered(rpcManager1);
      assert mBeanServer.isRegistered(rpcManager2);

      Object statsEnabled = mBeanServer.getAttribute(rpcManager1, "StatisticsEnabled");
      assert statsEnabled != null;
      assert statsEnabled.equals(Boolean.TRUE);

      assert mBeanServer.getAttribute(rpcManager1, "StatisticsEnabled").equals(Boolean.TRUE);
      assert mBeanServer.getAttribute(rpcManager2, "StatisticsEnabled").equals(Boolean.TRUE);

      // The initial state transfer uses cache commands, so it also increases the ReplicationCount value
      long initialReplicationCount1 = (Long) mBeanServer.getAttribute(rpcManager1, "ReplicationCount");

      cache1.put("key", "value2");
      assert cache2.get("key").equals("value2");
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals(initialReplicationCount1 + 1)
            : "Expected " + (initialReplicationCount1 + 1) + ", was " + mBeanServer.getAttribute(rpcManager1, "ReplicationCount");
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationFailures").equals((long) 0);

      // now reset statistics
      mBeanServer.invoke(rpcManager1, "resetStatistics", new Object[0], new String[0]);
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals((long) 0);
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationFailures").equals((long) 0);

      mBeanServer.setAttribute(rpcManager1, new Attribute("StatisticsEnabled", Boolean.FALSE));

      cache1.put("key", "value");
      assert cache2.get("key").equals("value");
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals((long) -1);
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals((long) -1);

      // reset stats enabled parameter
      mBeanServer.setAttribute(rpcManager1, new Attribute("StatisticsEnabled", Boolean.TRUE));
   }

   @Test(dependsOnMethods = "testEnableJmxStats")
   public void testSuccessRatio() throws Exception {
      Cache<String, Serializable> cache1 = manager(0).getCache(cachename);
      Cache cache2 = manager(1).getCache(cachename);
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName rpcManager1 = getCacheObjectName(JMX_DOMAIN, cachename + "(repl_sync)", "RpcManager");

      // the previous test has reset the statistics
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals((long) 0);
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationFailures").equals((long) 0);
      assert mBeanServer.getAttribute(rpcManager1, "SuccessRatio").equals("N/A");

      cache1.put("a1", new SlowToSerialize("b1", 50));
      cache1.put("a2", new SlowToSerialize("b2", 50));
      cache1.put("a3", new SlowToSerialize("b3", 50));
      cache1.put("a4", new SlowToSerialize("b4", 50));
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals((long) 4);
      assert mBeanServer.getAttribute(rpcManager1, "SuccessRatio").equals("100%");
      Object avgReplTime = mBeanServer.getAttribute(rpcManager1, "AverageReplicationTime");
      assert !avgReplTime.equals((long) 0) : "Expected !0, was " + avgReplTime;

      RpcManagerImpl rpcManager = (RpcManagerImpl) TestingUtil.extractComponent(cache1, RpcManager.class);
      Transport originalTransport = rpcManager.getTransport();

      try {
         Address mockAddress1 = mock(Address.class);
         Address mockAddress2 = mock(Address.class);
         List<Address> memberList = new ArrayList<Address>(2);
         memberList.add(mockAddress1);
         memberList.add(mockAddress2);
         Transport transport = mock(Transport.class);
         when(transport.getMembers()).thenReturn(memberList);
         when(transport.getAddress()).thenReturn(null);
         when(transport.invokeRemotely(any(Collection.class), any(ReplicableCommand.class), any(ResponseMode.class), anyLong(), anyBoolean(), any(ResponseFilter.class),
                                                  anyBoolean())).thenThrow(new RuntimeException());
         rpcManager.setTransport(transport);
         cache1.put("a5", "b5");
         assert false : "rpc manager should have thrown an exception";
      } catch (Throwable expected) {
         //expected
         assertEquals(mBeanServer.getAttribute(rpcManager1, "SuccessRatio"), ("80%"));
      }
      finally {
         rpcManager.setTransport(originalTransport);
      }
   }

   public static class SlowToSerialize implements Externalizable {
      String val;
      transient long delay;

      public SlowToSerialize() {
      }

      private SlowToSerialize(String val, long delay) {
         this.val = val;
         this.delay = delay;
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
         out.writeObject(val);
         TestingUtil.sleepThread(delay);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         val = (String) in.readObject();
         TestingUtil.sleepThread(delay);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         SlowToSerialize that = (SlowToSerialize) o;

         if (val != null ? !val.equals(that.val) : that.val != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         return val != null ? val.hashCode() : 0;
      }
   }

}
