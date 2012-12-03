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

package org.infinispan.server.hotrod

/**
 * Constant values
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
trait Constants {

   val MAGIC_REQ = 0xA0
   val MAGIC_RES = 0xA1
   val VERSION_10: Byte = 10
   val VERSION_11: Byte = 11
   val VERSION_12: Byte = 12
   val DEFAULT_HASH_FUNCTION_VERSION: Byte = 2

   val INFINITE_LIFESPAN = 0x01
   val INFINITE_MAXIDLE = 0x02
}