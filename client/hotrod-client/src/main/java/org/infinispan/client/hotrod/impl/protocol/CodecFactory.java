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

import org.infinispan.client.hotrod.impl.ConfigurationProperties;

/**
 * Code factory.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class CodecFactory {

   private static final Codec CODEC_10 = new Codec10();
   private static final Codec CODEC_11 = new Codec11();

   public static Codec getCodec(String version) {
      if (version.equals(ConfigurationProperties.PROTOCOL_VERSION_10))
         return CODEC_10;
      else if (version.equals(ConfigurationProperties.PROTOCOL_VERSION_11))
         return CODEC_11;
      else
         throw new IllegalArgumentException("Invalid Hot Rod protocol version");
   }

}
