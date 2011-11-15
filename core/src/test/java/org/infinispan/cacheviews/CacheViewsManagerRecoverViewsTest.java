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
package org.infinispan.cacheviews;

import org.easymock.EasyMock;
import org.infinispan.commands.control.CacheViewControlCommand;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.distribution.TestAddress;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifierImpl;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.easymock.EasyMock.*;

@Test(groups = "unit", testName = "cacheviews.CacheViewsManagerRecoverViewsTest")
public class CacheViewsManagerRecoverViewsTest extends AbstractInfinispanTest {

   private Address a1 = new TestAddress(1, "CacheViewsManagerRecoverViewsTest");
   private Address a2 = new TestAddress(2, "CacheViewsManagerRecoverViewsTest");
   private Address a3 = new TestAddress(3, "CacheViewsManagerRecoverViewsTest");
   private Address a4 = new TestAddress(4, "CacheViewsManagerRecoverViewsTest");

   public void testRecover1() throws Exception {
      Transport mockTransport = createStrictMock(Transport.class);
      makeThreadSafe(mockTransport, true);
      CacheViewListener mockListener = createStrictMock(CacheViewListener.class);
      makeThreadSafe(mockListener, true);

      List<Address> members1_1 = Arrays.asList(a1);
      CacheView v1_1 = new CacheView(1, members1_1);
      List<Address> members1_2 = Arrays.asList(a2, a3, a4);
      CacheView v1_2 = new CacheView(3, members1_2);
      List<Address> members2 = Arrays.asList(a1, a2, a3, a4);
      CacheView v2 = new CacheView(4, members2);
      Map<Address, Response> noResponse = new HashMap<Address, Response>();
      Map<Address, Response> nullResponses = buildMap(Arrays.asList(a2, a3, a4), Arrays.<Response>asList(null, null, null));

      // during start
      expect(mockTransport.getAddress()).andReturn(a1);
      expect(mockTransport.getMembers()).andReturn(members1_1);

      expect(mockTransport.getCoordinator()).andReturn(a1);
      expect(mockTransport.isCoordinator()).andReturn(true);

      // after join, installing the first view
      mockListener.prepareView(v1_1, CacheView.EMPTY_CACHE_VIEW);
      expect(mockTransport.invokeRemotely(eq(members1_1),
            isA(CacheViewControlCommand.class), eq(ResponseMode.SYNCHRONOUS),
            anyLong(), anyBoolean(), EasyMock.<ResponseFilter>isNull(), eq(false)))
            .andReturn(noResponse);
      mockListener.updateLeavers(Collections.<Address>emptySet());
      mockListener.commitView(v1_1.getViewId());
      expect(mockTransport.invokeRemotely(eq(members1_1),
            isA(CacheViewControlCommand.class), eq(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS),
            anyLong(), anyBoolean(), EasyMock.<ResponseFilter>isNull(), eq(false)))
            .andReturn(noResponse);

      // after the merge notification
      expect(mockTransport.getCoordinator()).andReturn(a1);
      expect(mockTransport.isCoordinator()).andReturn(true);

      // recovering the views
      Map<Address, Response> recoveredViews = buildMap(Arrays.asList(a2, a3, a4),
            Arrays.<Response>asList(new SuccessfulResponse(Collections.singletonMap("cache", v1_2)),
                  new SuccessfulResponse(Collections.singletonMap("cache", v1_2)),
                  new SuccessfulResponse(Collections.singletonMap("cache", v1_2))));
      expect(mockTransport.invokeRemotely(EasyMock.<Collection>isNull(),
            isA(CacheViewControlCommand.class), eq(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS),
            anyLong(), anyBoolean(), EasyMock.<ResponseFilter>isNull(), eq(false)))
            .andReturn(recoveredViews);

      // roll back each partition to the recovered view
      mockListener.rollbackView(v1_1.getViewId());
      expect(mockTransport.invokeRemotely(eq(members1_2),
            isA(CacheViewControlCommand.class), eq(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS),
            anyLong(), anyBoolean(), EasyMock.<ResponseFilter>isNull(), eq(false)))
            .andReturn(nullResponses);

      // installing the new merged view
      mockListener.prepareView(v2, v1_1);
      expect(mockTransport.invokeRemotely(eq(members1_2),
            isA(CacheViewControlCommand.class), eq(ResponseMode.SYNCHRONOUS),
            anyLong(), anyBoolean(), EasyMock.<ResponseFilter>isNull(), eq(false)))
            .andReturn(nullResponses);
      mockListener.updateLeavers(Collections.<Address>emptySet());
      mockListener.commitView(v2.getViewId());
      expect(mockTransport.invokeRemotely(eq(members1_2),
            isA(CacheViewControlCommand.class), eq(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS),
            anyLong(), anyBoolean(), EasyMock.<ResponseFilter>isNull(), eq(false)))
            .andReturn(nullResponses);

      replay(mockTransport, mockListener);

      CacheManagerNotifierImpl notifier = new CacheManagerNotifierImpl();
      notifier.start();
      ExecutorService asyncTransportExecutor = new WithinThreadExecutor();
      GlobalConfiguration gc = new GlobalConfiguration();
      CacheViewsManagerImpl cvm = new CacheViewsManagerImpl();
      cvm.init(notifier, mockTransport, asyncTransportExecutor, gc);
      cvm.start();
      try {
         cvm.join("cache", mockListener);
         Thread.sleep(1000);
         notifier.notifyMerge(members2, members1_1, a1, 3, Arrays.asList(members1_1, members1_2));
         Thread.sleep(1000);
         verify(mockTransport, mockListener);
      } finally {
         cvm.stop();
      }
   }

   private <K, V> Map<K, V> buildMap(List<? extends K> keys, List<? extends V> values) {
      Map<K, V> map = new HashMap<K, V>();
      for (int i = 0; i < keys.size(); i++) {
         map.put(keys.get(i), values.get(i));
      }
      return map;
   }
}
