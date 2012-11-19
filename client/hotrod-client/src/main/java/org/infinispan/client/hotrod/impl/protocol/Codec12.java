/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * A Hot Rod encoder/decoder for version 1.2 of the protocol.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class Codec12 extends Codec11 {

   private static final Log log = LogFactory.getLog(Codec12.class, Log.class);

   @Override
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_12);
   }

   @Override
   protected HeaderParams writeHeader(Transport transport, HeaderParams params, byte version) {
      transport.writeByte(HotRodConstants.REQUEST_MAGIC);
      transport.writeVLong(params.messageId(MSG_ID.incrementAndGet()).messageId);
      transport.writeByte(version);
      transport.writeByte(params.opCode);
      transport.writeArray(params.cacheName);

      int flagInt = 0;
      if (params.flags != null) {
         for (Flag flag : params.flags) {
            flagInt = flag.getFlagInt() | flagInt;
         }
      }
      transport.writeVInt(flagInt);
      transport.writeByte(params.clientIntel);
      transport.writeVInt(params.topologyId.get());
      //todo change once TX support is added
      transport.writeByte(params.txMarker);
      getLog().tracef("Wrote header for message %d. Operation code: %#04x. Flags: %#x", params.messageId, params.opCode, flagInt);
      return params;
   }
}
