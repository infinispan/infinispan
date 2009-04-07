/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.marshall;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class EntryDataExceptionMarker implements Externalizable {
   private static final long serialVersionUID = 240199474174502551L;
   private Throwable cause;
   private Object key;

   public EntryDataExceptionMarker(Throwable t, Object key) {
      cause = t;
      key = key;
   }

   public Throwable getCause() {
      return cause;
   }

   public Object getKey() {
      return key;
   }

   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(cause);
      out.writeObject(key);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      cause = (Throwable) in.readObject();
      key = in.readObject();
   }
}