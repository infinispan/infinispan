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
package org.infinispan.client.hotrod.exceptions;

/**
 * Signals an remote timeout(due to locking) in the infinispan server. 
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class HotRodTimeoutException extends HotRodClientException {
   public HotRodTimeoutException() {
   }

   public HotRodTimeoutException(String message) {
      super(message);
   }

   public HotRodTimeoutException(Throwable cause) {
      super(cause);
   }

   public HotRodTimeoutException(String message, Throwable cause) {
      super(message, cause);
   }

   public HotRodTimeoutException(String remoteMessage, long messageId, int errorStatusCode) {
      super(remoteMessage, messageId, errorStatusCode);
   }
}
