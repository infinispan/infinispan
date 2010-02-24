/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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
package org.infinispan.server.memcached.test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.server.memcached.TextServer;

import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;

/**
 * TestingUtil.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
public class MemcachedTestingUtil {
   private static final String HOST = "127.0.0.1";

   private static final ThreadLocal<Integer> threadMemcachedPort = new ThreadLocal<Integer>() {
      private final AtomicInteger uniqueAddr = new AtomicInteger(11211);

      @Override
      protected Integer initialValue() {
         return uniqueAddr.getAndAdd(100);
      }
   };

   public static String k(Method method, String prefix) {
      return prefix + method.getName();
   }

   public static Object v(Method method, String prefix) {
      return prefix  + method.getName();
   }

   public static String k(Method method) {
      return k(method, "k-");
   }

   public static Object v(Method method) {
      return v(method, "v-");
   }

   public static MemcachedClient createMemcachedClient(final long timeout, final int port) throws IOException {
      DefaultConnectionFactory d = new DefaultConnectionFactory() {
         @Override
         public long getOperationTimeout() {
            return timeout;
         }
      };
      return new MemcachedClient(d, Arrays.asList(new InetSocketAddress(HOST, port)));
   }

   public static TextServer createMemcachedTextServer(Cache cache) throws IOException {
      return new TextServer(HOST, threadMemcachedPort.get().intValue(), cache, 0, 0);
   }

   public static TextServer createMemcachedTextServer(Cache cache, int port) throws IOException {
      return new TextServer(HOST, port, cache, 0, 0);
   }
}
