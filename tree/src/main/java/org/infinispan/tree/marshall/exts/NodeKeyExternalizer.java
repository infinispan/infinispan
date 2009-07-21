/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.tree.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import static org.infinispan.tree.NodeKey.Type.*;

import org.infinispan.marshall.Externalizer;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.NodeKey;

/**
 * NodeKeyExternalizer.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 * @deprecated Externalizer implementation now within NodeKey
 */
@Deprecated
public class NodeKeyExternalizer implements Externalizer {
   private static final byte DATA_BYTE = 1;
   private static final byte STRUCTURE_BYTE = 2;

   public void writeObject(ObjectOutput output, Object object) throws IOException {
      NodeKey key = (NodeKey) object;
      output.writeObject(key.getFqn());
      byte type = 0;
      switch (key.getContents()) {
         case DATA:
            type = DATA_BYTE;
            break;
         case STRUCTURE:
            type = STRUCTURE_BYTE;
            break;
      }
      output.write(type);
   }
   
   public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Fqn fqn = (Fqn) input.readObject();
      int typeb = input.readUnsignedByte();
      NodeKey.Type type = null; 
      switch (typeb) {
         case DATA_BYTE:
            type = DATA;
            break;
         case STRUCTURE_BYTE:
            type = STRUCTURE;
            break;
      }
      return new NodeKey(fqn, type);
   }

}
