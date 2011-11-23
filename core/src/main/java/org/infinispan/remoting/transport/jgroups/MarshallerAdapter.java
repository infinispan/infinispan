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
package org.infinispan.remoting.transport.jgroups;

import org.infinispan.api.io.ByteBuffer;
import org.infinispan.api.marshall.StreamingMarshaller;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Buffer;

/**
 * Bridge between JGroups and Infinispan marshallers
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MarshallerAdapter implements RpcDispatcher.Marshaller {
   StreamingMarshaller m;

   public MarshallerAdapter(StreamingMarshaller m) {
      this.m = m;
   }

   @Override
   public Buffer objectToBuffer(Object obj) throws Exception {
      return toBuffer(m.objectToBuffer(obj));
   }

   @Override
   public Object objectFromBuffer(byte[] buf, int offset, int length) throws Exception {
      return m.objectFromByteBuffer(buf, offset, length);
   }

   private Buffer toBuffer(ByteBuffer bb) {
      return new Buffer(bb.getBuf(), bb.getOffset(), bb.getLength());
   }
}
