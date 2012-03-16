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
package org.infinispan.test.fwk;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.fail;

/**
 * The purpose of this class is to test that/if tcp + mping works fine in the given environment.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(testName = "test.fwk.TcpMPingEnvironmentTest", groups = "functional", enabled = false,
      description = "This test just tests whether the HUdson environment allows proper binding to UDP sockets.")
public class TcpMPingEnvironmentTest {

   Log log = LogFactory.getLog(TcpMPingEnvironmentTest.class);

   List<JChannel> openedChannles = new ArrayList<JChannel>();
   private boolean success = false;
   private static final String IP_ADDRESS = "228.10.10.5";

   @AfterMethod
   public void destroyCaches() {
      for (JChannel ch : openedChannles) {
         ch.disconnect();
         ch.close();
      }
//      tryPrintRoutingInfo();
      if (!success) {
         Properties properties = System.getProperties();
         log.trace("System props are " + properties);
         System.out.println("System props are " + properties);
         tryPrintRoutingInfo();
      }
   }

   private void tryPrintRoutingInfo() {
      tryExecNativeCommand("/sbin/route", "Routing table is ");
      tryExecNativeCommand("/sbin/ip route get 228.10.10.5", "/sbin/ip route get " + IP_ADDRESS);
   }

   private void tryExecNativeCommand(String command, String printPrefix) {
      try {
         Process p = Runtime.getRuntime().exec(command);
         InputStream inputStream = p.getInputStream();
         BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

         String line = reader.readLine();
         StringBuilder result = new StringBuilder();
         while (line != null) {
            result.append(line).append('\n');
            line = reader.readLine();
         }
         log.trace(printPrefix + result);
         inputStream.close();
      } catch (IOException e) {
         log.trace("Cannot print " + printPrefix + " !",e);
      }
   }

   /**
    * Tests that different clusters are created and that they don't overlap.
    */
   public void testDifferentClusters() throws Exception {
      JChannel first1 = new JChannel("stacks/tcp_mping/tcp1.xml");
      JChannel first2 = new JChannel("stacks/tcp_mping/tcp1.xml");
      JChannel first3 = new JChannel("stacks/tcp_mping/tcp1.xml");
      initiChannel(first1);
      initiChannel(first2);
      initiChannel(first3);

      expectView(first1, first2, first3);

      JChannel second1 = new JChannel("stacks/tcp_mping/tcp2.xml");
      JChannel second2 = new JChannel("stacks/tcp_mping/tcp2.xml");
      JChannel second3 = new JChannel("stacks/tcp_mping/tcp2.xml");
      initiChannel(second1);
      initiChannel(second2);
      initiChannel(second3);

      expectView(first1, first2, first3);
      expectView(second1, second2, second3);
      success = true;
   }


   public void testMcastSocketCreation() throws Exception {
      InetAddress mcast_addr = InetAddress.getByName(IP_ADDRESS);
      SocketAddress saddr = new InetSocketAddress(mcast_addr, 43589);
      MulticastSocket retval = null;
      try {
         success = false;
         retval = new MulticastSocket(saddr);
         success = true;
      } finally {
         if (retval != null) {
            try {
               retval.close();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      }
   }

   public void testMcastSocketCreation2() throws Exception {
      InetAddress mcast_addr = InetAddress.getByName(IP_ADDRESS);
      int port = 43589;
      MulticastSocket retval = null;
      try {
         Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
         StringBuilder okTrace = new StringBuilder();
         StringBuilder failureTrace = new StringBuilder();
         success = true;
         while (nis.hasMoreElements()) {
            retval = new MulticastSocket(port);
            NetworkInterface networkInterface = nis.nextElement();
            retval.setNetworkInterface(networkInterface);
            try {
               retval.joinGroup(mcast_addr);
               String msg = "Successfully bind to " + networkInterface;
               okTrace.append(msg).append('\n');
            } catch (IOException e) {
               e.printStackTrace();
               String msg = "Failed to bind to " + networkInterface + ".";
               failureTrace.append(msg).append('\n');
               success = false;
            }
         }
         if (success) {
            log.trace(okTrace);
            System.out.println("Sucessfull binding! " + okTrace);
         } else {
            String message = "Success : " + okTrace + ". Failures : " + failureTrace;
            log.error(message);
            System.out.println(message);
            throw new RuntimeException(message);
         }
      } finally {
         if (retval != null) {
            try {
               retval.close();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      }
   }


   private void expectView(JChannel... channels) throws Exception {
      for (int i = 0; i < 20; i++) {
         boolean success = true;
         for (int j = 0; j < channels.length; j++) {
            View view = channels[j].getView();
            if (view == null) {
               success = false;
               break;
            }
            success = success && (view.size() == channels.length);
            if (view.size() > channels.length) assert false : "Clusters see each other!";
         }
         if (success) return;
         Thread.sleep(1000);
      }
      fail("Could not form cluster in given timeout");
   }


   private void initiChannel(JChannel channel) throws Exception {
      openedChannles.add(channel);
      channel.setDiscardOwnMessages(true);
      channel.connect("someChannel");
   }
}
