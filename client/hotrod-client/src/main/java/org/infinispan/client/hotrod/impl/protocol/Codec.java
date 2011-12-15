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

package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;

/**
 * A Hot Rod protocol encoder/decoder.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface Codec {

   /**
    * Writes a request header with the given parameters to the transport and
    * returns an updated header parameters.
    */
   HeaderParams writeHeader(Transport transport, HeaderParams params);

   /**
    * Reads a response header from the transport and returns the status
    * of the response.
    */
   short readHeader(Transport transport, HeaderParams params);

   /**
    * Logger for Hot Rod client codec
    */
   Log getLog();

}
