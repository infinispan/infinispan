/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.io;


import java.io.OutputStream;

import org.jboss.marshalling.ByteOutput;

/**
 * A stream of bytes which can be written to, and the underlying byte array can be directly accessed.
 * 
 * By implementing {@link org.jboss.marshalling.ByteOutput} we avoid the need for the byte stream to be wrapped by
 * {@link org.jboss.marshalling.Marshalling#createByteOutput(OutputStream)}
 *
 * @author Manik Surtani
 * @author Sanne Grinovero
 * @since 5.1
 */
public abstract class MarshalledValueByteStream extends OutputStream implements ByteOutput {

   public abstract int size();

   public abstract byte[] getRaw();

}
