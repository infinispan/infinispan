/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.interceptors;

import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.MarshalledValue;

/**
 * A marshalled value interceptor which forces defensive copies to be made
 * proactively. By doing so, clients are no longer able to make any changes
 * via direct object references, so any changes require a cache modification
 * call via put/replace...etc methods.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class DefensiveMarshalledValueInterceptor extends MarshalledValueInterceptor {

   @Override
   protected void compact(MarshalledValue mv) {
      // Force marshalled version to be stored
      if (mv != null)
         mv.compact(true, true);
   }

   @Override
   protected Object processRetVal(Object retVal, InvocationContext ctx) {
      Object ret = retVal;
      if (retVal instanceof MarshalledValue) {
         // Calculate return
         ret = super.processRetVal(ret, ctx);
         // Re-compact in case deserialization happened
         ((MarshalledValue) retVal).compact(true, true);
      }
      return ret;
   }

}
