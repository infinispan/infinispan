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

package org.infinispan.remoting.jgroups;

import org.infinispan.test.AbstractInfinispanTest;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.View;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.RspList;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Test(testName = "remoting.ConcurrentChannelStartTest", groups = "functional")
public class ConcurrentChannelStartTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(RpcDispatcherServer.class);
   private static Method REMOTE_METHOD;

   static {
      try {
         REMOTE_METHOD = RpcDispatcherServer.class.getMethod("remoteMethod", String.class);
      } catch (NoSuchMethodException e) {
         e.printStackTrace();
      }
   }

   public void testSequentialStart() throws Throwable {
      ArrayList<RpcDispatcherServer> servers = new ArrayList<RpcDispatcherServer>();

      for (int i = 0; i < 5; i++) {
         RpcDispatcherServer server = createChannel(i);
         servers.add(server);
      }

      for (int i = 0; i < 5; i++) {
         RpcDispatcherServer server = servers.get(i);
         server.ch.connect("ConcurrentChannelStartTest");
      }
//
//      for (int j = 1; j < 5; j++) {
//         int expected = (servers.size() - j) * (servers.size() - j - 1) / 2;
//         int actual = servers.get(j).remoteMethodInvocationCount();
//         assert actual == expected : String.format("Expected %d invocations, got %d", expected, actual);
//      }
   }

   private RpcDispatcherServer createChannel(int i) throws Throwable {
      int port = 7800 + i;
//      String ping = "TCPPING(ergonomics=false;initial_hosts=localhost[7800])";
      String ping = "org.infinispan.test.fwk.TEST_PING(ergonomics=false;testName=)";
      String config = "TCP(bind_port=" + port + ";discard_incompatible_packets=true;enable_bundling=true;" +
            "enable_diagnostics=false;loopback=true;max_bundle_size=64000;max_bundle_timeout=30;" +
            "oob_thread_pool.enabled=true;oob_thread_pool.keep_alive_time=5000;oob_thread_pool.max_threads=8;" +
            "oob_thread_pool.min_threads=2;oob_thread_pool.queue_enabled=false;oob_thread_pool.queue_max_size=100;" +
            "oob_thread_pool.rejection_policy=Run;port_range=30;recv_buf_size=20000000;send_buf_size=640000;" +
            "sock_conn_timeout=300;thread_pool.enabled=true;thread_pool.keep_alive_time=5000;thread_pool.max_threads=8;" +
            "thread_pool.min_threads=2;thread_pool.queue_enabled=false;thread_pool.queue_max_size=100;" +
            "thread_pool.rejection_policy=Run;use_send_queues=true):" + ping + ":" +
            "MERGE2(max_interval=30000;min_interval=10000):pbcast.NAKACK(discard_delivered_msgs=false;gc_lag=0;" +
            "retransmit_timeout=300,600,1200,2400,4800;use_mcast_xmit=false):UNICAST(timeout=300,600,1200):" +
            "pbcast.STABLE(desired_avg_gossip=50000;max_bytes=400000;stability_delay=1000):" +
            "pbcast.GMS(join_timeout=7000;print_local_addr=false;view_bundling=true):FC(max_credits=2000000;min_threshold=0.10):" +
            "FRAG2(frag_size=60000):pbcast.STREAMING_STATE_TRANSFER";
      JChannel ch = new JChannel(config);
      RpcDispatcherServer server = new RpcDispatcherServer(ch);
      RpcDispatcher disp = new RpcDispatcher(ch, server, server, server);
      server.disp = disp;
      return server;
   }


   public class RpcDispatcherServer implements MessageListener, MembershipListener {

      //ExecutorService executor = new WithinThreadExecutor();
      ExecutorService executor = Executors.newCachedThreadPool();

      private final JChannel ch;
      private volatile int invocationCount;
      public RpcDispatcher disp;

      public RpcDispatcherServer(JChannel ch) {
         this.ch = ch;
      }

      @Override
      public void viewAccepted(final View new_view) {
         log.info(String.format("viewAccepted: %s", new_view));
         executor.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
               try {
                  Thread.sleep(10);
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               }
               try {
                  broadcastRemoteMethod(new_view.getMembers());
               } catch (NoSuchMethodException e) {
                  log.error("Error broadcasting message", e);
               }
               return null;
            }
         });
      }

      @Override
      public void suspect(Address suspected_mbr) {
         log.info(String.format("suspect: %s", suspected_mbr));
      }

      @Override
      public void block() {
         log.info(String.format("block"));
      }

      @Override
      public void receive(Message msg) {
         log.info(String.format("receive: %s", msg));
      }
   
      @Override
      public byte[] getState() {
         return new byte[0];
      }

      @Override
      public void setState(byte[] state) {
      }

      public int remoteMethod(String sender) {
         log.info(String.format("remoteMethod called on %s by %s", ch.getAddress(), sender));
         invocationCount++;
         return invocationCount;
      }

      private void broadcastRemoteMethod(List<Address> targets) throws NoSuchMethodException {
         MethodCall method = new MethodCall(REMOTE_METHOD, ch.getAddressAsString());
         log.info("calling remoteMethod on " + targets);
         RspList rspList = disp.callRemoteMethods(targets, method, RequestOptions.SYNC());
         log.info("remoteMethod invocation results: " + rspList);
         assert rspList.size() == targets.size() : String.format("expected %d responses, got only %d", targets.size(), rspList.size());
         for (int i = 0; i < rspList.size(); i++) {
            assert rspList.get(targets.get(i)).wasReceived() : String.format("response from %s was not received", targets.get(i));
         }
      }

      public int remoteMethodInvocationCount() {
         return invocationCount;
      }
   }
}
