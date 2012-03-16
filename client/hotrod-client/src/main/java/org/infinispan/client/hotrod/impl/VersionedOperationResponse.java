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
package org.infinispan.client.hotrod.impl;

/**
* @author Mircea.Markus@jboss.com
* @since 4.1
*/
public class VersionedOperationResponse {

   public enum RspCode {
      SUCCESS(true), NO_SUCH_KEY(false), MODIFIED_KEY(false);
      private boolean isModified;

      RspCode(boolean modified) {
         isModified = modified;
      }

      public boolean isUpdated() {
         return isModified;
      }
   }

   private byte[] value;

   private RspCode code;


   public VersionedOperationResponse(byte[] value, RspCode code) {
      this.value = value;
      this.code = code;
   }

   public byte[] getValue() {
      return value;
   }

   public RspCode getCode() {
      return code;
   }
}
