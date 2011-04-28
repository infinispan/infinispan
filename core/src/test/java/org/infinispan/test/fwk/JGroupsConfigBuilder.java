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

import org.infinispan.config.parsing.JGroupsStackParser;
import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.util.LegacyKeySupportSystemProperties;
import org.jgroups.util.Util;
import org.w3c.dom.Element;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class owns the logic of associating network resources(i.e. ports) with threads, in order to make sure that there
 * won't be any clashes between multiple clusters running in parallel on same host. Used for parallel test suite.
 *
 * @author Mircea.Markus@jboss.com
 */
public class JGroupsConfigBuilder {

   public static final String JGROUPS_STACK;

   private static String bind_addr = "127.0.0.1";
   private static String tcpConfig;
   private static String udpConfig;

   private static final ThreadLocal<String> threadTcpStartPort = new ThreadLocal<String>() {
      private final AtomicInteger uniqueAddr = new AtomicInteger(7900);

      @Override
      protected String initialValue() {
         return String.valueOf(uniqueAddr.getAndAdd(50));
      }
   };

   /**
    * Holds unique mcast_addr for each thread used for JGroups channel construction.
    */
   private static final ThreadLocal<String> threadMcastIP = new ThreadLocal<String>() {
      private final AtomicInteger uniqueAddr = new AtomicInteger(11);

      @Override
      protected String initialValue() {
         return "228.10.10." + uniqueAddr.getAndIncrement();
      }
   };

   /**
    * Holds unique mcast_port for each thread used for JGroups channel construction.
    */
   private static final ThreadLocal<Integer> threadMcastPort = new ThreadLocal<Integer>() {
      private final AtomicInteger uniquePort = new AtomicInteger(45589);

      @Override
      protected Integer initialValue() {
         return uniquePort.getAndIncrement();
      }
   };

   private static final Pattern TCP_START_PORT = Pattern.compile("bind_port=[^;]*");
   private static final Pattern TCP_INITIAL_HOST = Pattern.compile("initial_hosts=[^;]*");
   private static final Pattern UDP_MCAST_ADDRESS = Pattern.compile("mcast_addr=[^;]*");
   private static final Pattern UDP_MCAST_PORT = Pattern.compile("mcast_port=[^;]*");

   static {
      JGROUPS_STACK = LegacyKeySupportSystemProperties.getProperty("infinispan.test.jgroups.protocol", "protocol.stack", "tcp");
      System.out.println("Transport protocol stack used = " + JGROUPS_STACK);

      try {
         bind_addr = Util.getBindAddress(null).getHostAddress();
      } catch (Exception e) {
      }
   }

   public static String getJGroupsConfig() {
      if (JGROUPS_STACK.equalsIgnoreCase("tcp")) return getTcpConfig();
      if (JGROUPS_STACK.equalsIgnoreCase("udp")) return getUdpConfig();
      throw new IllegalStateException("Unknown protocol stack : " + JGROUPS_STACK);
   }

   public static String getTcpConfig() {
      loadTcp();

      if (tcpConfig.contains("TCPPING")) {
         return getTcpConfigWithTCPPINGDiscovery();
      } else {
         return replaceMCastAddressAndPort(tcpConfig);
      }
   }

   private static String getTcpConfigWithTCPPINGDiscovery() {
      // replace mcast_addr
      Matcher m = TCP_START_PORT.matcher(tcpConfig);
      String result;
      String newStartPort;
      if (m.find()) {
         newStartPort = threadTcpStartPort.get();
         result = m.replaceFirst("bind_port=" + newStartPort);
      } else {
         System.out.println("Config is:" + tcpConfig);
         Thread.dumpStack();
         throw new IllegalStateException();
      }

      if (result.indexOf("TCPGOSSIP") < 0) // onluy adjust for TCPPING
      {
         m = TCP_INITIAL_HOST.matcher(result);
         if (m.find()) {
            result = m.replaceFirst("initial_hosts=" + bind_addr + "[" + newStartPort + "]");
         }
      }
      return result;
   }

   public static String getUdpConfig() {
      loadUdp();
      // replace mcast_addr
      String config = udpConfig;
      return replaceMCastAddressAndPort(config);
   }

   private static String replaceMCastAddressAndPort(String config) {
      Matcher m = UDP_MCAST_ADDRESS.matcher(config);
      String result;
      if (m.find()) {
         String newAddr = threadMcastIP.get();
         result = m.replaceFirst("mcast_addr=" + newAddr);
      } else {
         Thread.dumpStack();
         throw new IllegalStateException();
      }

      // replace mcast_port
      m = UDP_MCAST_PORT.matcher(result);
      if (m.find()) {
         String newPort = threadMcastPort.get().toString();
         result = m.replaceFirst("mcast_port=" + newPort);
      }
      return result;
   }

   private static void loadTcp() {
      if (tcpConfig != null) return;
      String xmlString = readFile("stacks/tcp.xml");
      tcpConfig = getJgroupsConfig(xmlString);
   }

   private static void loadUdp() {
      if (udpConfig != null) return;
      String xmlString = readFile("stacks/udp.xml");
      udpConfig = getJgroupsConfig(xmlString);
   }

   private static String getJgroupsConfig(String xmlString) {
      try {
         Element e = XmlConfigHelper.stringToElement(xmlString);
         JGroupsStackParser parser = new JGroupsStackParser();
         return parser.parseClusterConfigXml(e);
      } catch (Exception ex) {
         throw new RuntimeException("Unexpected!", ex);
      }
   }

   private static String readFile(String fileName) {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      InputStream is = cl.getResourceAsStream(fileName);
      BufferedReader bf = new BufferedReader(new InputStreamReader(is));
      StringBuilder result = new StringBuilder();
      String line;
      try {
         while ((line = bf.readLine()) != null) result.append(line);
      } catch (IOException e) {
         throw new RuntimeException("Unexpected!", e);
      } finally {
         try {
            bf.close();
         } catch (IOException e) {
            e.printStackTrace();
         }
      }
      return result.toString();
   }
}
