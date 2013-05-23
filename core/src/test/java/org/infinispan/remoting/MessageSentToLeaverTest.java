/*
 * JBoss, Home of Professional Open Source 
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.remoting;

import org.infinispan.Cache;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Map;

/**
 * Test that CommandAwareRpcManager detects members who left the cluster and throws an exception.
 *
 * @author Dan Berindei <dan@infinispan.org>
 */
@Test (testName = "remoting.MessageSentToLeaverTest", groups = "functional")
public class MessageSentToLeaverTest extends AbstractInfinispanTest {

   public void testGroupRequestSentToMemberAfterLeaving() {
      EmbeddedCacheManager cm1 = null, cm2 = null, cm3 = null;
      try {
         Configuration c = new Configuration().fluent()
               .mode(Configuration.CacheMode.DIST_SYNC)
               .hash().numOwners(3)
               .build();
         GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();

         cm1 = TestCacheManagerFactory.createCacheManager(gc, c);
         cm2 = TestCacheManagerFactory.createCacheManager(gc, c);
         cm3 = TestCacheManagerFactory.createCacheManager(gc, c);

         Cache<Object,Object> c1 = cm1.getCache();
         Cache<Object, Object> c2 = cm2.getCache();
         Cache<Object, Object> c3 = cm3.getCache();
         
         TestingUtil.blockUntilViewsReceived(30000, c1, c2, c3);

         c2.put("k", "v1");

         RpcManager rpcManager = TestingUtil.extractComponent(c1, RpcManager.class);
         Collection<Address>  addresses = cm1.getMembers();

         CommandsFactory cf = TestingUtil.extractCommandsFactory(c1);
         PutKeyValueCommand cmd = cf.buildPutKeyValueCommand("k", "v2",
               new EmbeddedMetadata.Builder().build(), null);

         Map<Address,Response> responseMap = rpcManager.invokeRemotely(addresses, cmd, rpcManager.getDefaultRpcOptions(true, false));
         assert responseMap.size() == 2;
         
         TestingUtil.killCacheManagers(cm2);
         TestingUtil.blockUntilViewsReceived(30000, false, c1, c3);
         
         try {
            responseMap = rpcManager.invokeRemotely(addresses, cmd, rpcManager.getDefaultRpcOptions(true, false));
            assert false: "invokeRemotely should have thrown an exception";
         } catch (SuspectException e) {
            // expected
         }
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2, cm3);
      }
   }

}
