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
package org.infinispan.loaders.keymappers;

import org.infinispan.loaders.CacheLoaderException;

/**
 * Exception thrown by certain cache stores when one tries to persist an entry with an unsupported key type.
 *
 * @author Mircea.Markus@jboss.com
 */
public class UnsupportedKeyTypeException extends CacheLoaderException {

   /** The serialVersionUID */
   private static final long serialVersionUID = 1442739860198872706L;

   public UnsupportedKeyTypeException(Object key) {
      this("Unsupported key type: '" + key.getClass().getName() + "' on key: " + key);
   }

   public UnsupportedKeyTypeException(String message) {
      super(message);
   }

   public UnsupportedKeyTypeException(String message, Throwable cause) {
      super(message, cause);
   }

   public UnsupportedKeyTypeException(Throwable cause) {
      super(cause);
   }
}
