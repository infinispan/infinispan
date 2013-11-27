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
package org.infinispan.context.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;

/**
 * Common features of transaction and invocation contexts
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class AbstractInvocationContext implements InvocationContext {

   private boolean isOriginLocal = false;
   private Address origin;
   // Class loader associated with this invocation which supports AdvancedCache.with() functionality
   private ClassLoader classLoader;

   @Override
   public final Address getOrigin() {
	   return origin;
   }

   public final void setOrigin(Address origin) {
	   this.origin = origin;
   }

   @Override
   public boolean isOriginLocal() {
      return isOriginLocal;
   }

   public void setOriginLocal(boolean isOriginLocal) {
      this.isOriginLocal = isOriginLocal;
   }

   @Override
   public boolean hasLockedKey(Object key) {
      return getLockedKeys().contains(key);
   }

   @Override
   public AbstractInvocationContext clone() {
      try {
         return (AbstractInvocationContext) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!");
      }
   }

   @Override
   public final ClassLoader getClassLoader() {
      return classLoader;
   }

   @Override
   public final void setClassLoader(final ClassLoader classLoader) {
      this.classLoader = classLoader;
   }

   @Override
   public boolean replaceValue(Object key, Object value) {
      CacheEntry ce = lookupEntry(key);
      if (ce == null || ce.isNull() || ce.isLockPlaceholder() || ce.getValue() == null) {
         if (ce != null && ce.isChanged()) {
            ce.setValue(value);
         } else {
            return false;
         }
      }
      return true;
   }
}
