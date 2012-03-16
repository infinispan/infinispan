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
package org.infinispan.client.hotrod;

import java.util.Map;

/**
 * Defines all the flags available in the Hot Rod client that can influence the behavior of operations.
 * <p />
 * Available flags:
 * <ul>
 *    <li>{@link #FORCE_RETURN_VALUE} - By default, previously existing values for {@link Map} operations are not
 *                                      returned. E.g. {@link RemoteCache#put(Object, Object)} does <i>not</i> return
 *                                      the previous value associated with the key.  By applying this flag, this default
 *                                      behavior is overridden for the scope of a single invocation, and the previous
 *                                      existing value is returned.</li>
 * </ul>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public enum Flag {

   /**
    * By default, previously existing values for {@link Map} operations are not returned. E.g. {@link RemoteCache#put(Object, Object)}
    * does <i>not</i> return the previous value associated with the key.
    * <p />
    * By applying this flag, this default behavior is overridden for the scope of a single invocation, and the previous
    * existing value is returned.
    */
   FORCE_RETURN_VALUE(0x0001);

   private int flagInt;

   Flag(int flagInt) {
      this.flagInt = flagInt;
   }

   public int getFlagInt() {
      return flagInt;
   }
}
