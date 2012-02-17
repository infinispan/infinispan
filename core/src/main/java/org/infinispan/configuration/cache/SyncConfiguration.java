/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.cache;

/**
 * If configured all communications are synchronous, in that whenever a thread sends a message sent
 * over the wire, it blocks until it receives an acknowledgment from the recipient. SyncConfig is
 * mutually exclusive with the AsyncConfig.
 */
public class SyncConfiguration {

   private long replTimeout;


   SyncConfiguration(long replTimeout) {
      this.replTimeout = replTimeout;
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public long replTimeout() {
      return replTimeout;
   }
   
   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public SyncConfiguration replTimeout(long l) {
      this.replTimeout = l;
      return this;
   }

   @Override
   public String toString() {
      return "SyncConfiguration{" +
            "replTimeout=" + replTimeout +
            '}';
   }

}
