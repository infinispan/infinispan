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
 * Indicates a communication exception with the Hot Rod server: e.g. TCP connection is broken while reading a response
 * from the server.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TransportException extends HotRodClientException {
   public TransportException() {
   }

   public TransportException(String message) {
      super(message);
   }

   public TransportException(String message, Throwable cause) {
      super(message, cause);
   }

   public TransportException(Throwable cause) {
      super(cause);
   }
}