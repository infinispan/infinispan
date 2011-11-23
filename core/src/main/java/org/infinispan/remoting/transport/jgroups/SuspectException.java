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
package org.infinispan.remoting.transport.jgroups;

import org.infinispan.api.CacheException;
import org.infinispan.remoting.transport.Address;

/**
 * Thrown when a member is suspected during remote method invocation
 *
 * @author Bela Ban
 * @author Galder Zamarreño
 * @since 4.0
 */
public class SuspectException extends CacheException {

   private static final long serialVersionUID = -2965599037371850141L;
   private final Address suspect;

   public SuspectException() {
      super();
      this.suspect = null;
   }

   public SuspectException(String msg) {
      super(msg);
      this.suspect = null;
   }

   public SuspectException(String msg, Address suspect) {
      super(msg);
      this.suspect = suspect;
   }

   public SuspectException(String msg, Throwable cause) {
      super(msg, cause);
      this.suspect = null;
   }

   public SuspectException(String msg, Address suspect, Throwable cause) {
      super(msg, cause);
      this.suspect = suspect;
   }

   public Address getSuspect() {
      return suspect;
   }

}
