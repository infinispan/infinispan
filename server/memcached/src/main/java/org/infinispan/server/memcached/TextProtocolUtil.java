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
package org.infinispan.server.memcached;

/**
 * TextProtocolUtil.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class TextProtocolUtil {
   static final byte CR = 13;
   static final byte LF = 10;
   static final byte[] CRLF = new byte[] { CR, LF };

   public static byte[] concat(byte[] a, byte[] b) {
      byte[] data = new byte[a.length + b.length];
      System.arraycopy(a, 0, data, 0, a.length);
      System.arraycopy(b, 0, data, a.length , b.length);
      return data;
   }
}
