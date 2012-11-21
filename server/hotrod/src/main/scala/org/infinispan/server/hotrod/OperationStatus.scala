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
package org.infinispan.server.hotrod

/**
 * Hot Rod operation possible status outcomes.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object OperationStatus extends Enumeration {
   type OperationStatus = Value

   val Success = Value(0x00)
   val OperationNotExecuted = Value(0x01)
   val KeyDoesNotExist = Value(0x02)

   val InvalidMagicOrMsgId = Value(0x81)
   val UnknownOperation = Value(0x82)
   val UnknownVersion = Value(0x83) // todo: test
   val ParseError = Value(0x84) // todo: test
   val ServerError = Value(0x85) // todo: test
   val OperationTimedOut = Value(0x86) // todo: test

}
