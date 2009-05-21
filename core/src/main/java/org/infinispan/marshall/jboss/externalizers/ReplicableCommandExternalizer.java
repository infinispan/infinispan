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
package org.infinispan.marshall.jboss.externalizers;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.marshall.jboss.ClassExternalizer;
import org.infinispan.marshall.jboss.Externalizer;
import org.infinispan.util.Util;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;

/**
 * ReplicableCommandExternalizer.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class ReplicableCommandExternalizer implements Externalizer, ClassExternalizer.ClassWritable {
   /** The serialVersionUID */
   private static final long serialVersionUID = 6915200269446867084L;
   private ClassExternalizer classExt;

   public void writeObject(Marshaller output, Object subject) throws IOException {
      writeClass(output, subject.getClass());
      ReplicableCommand command = (ReplicableCommand) subject;
      output.writeShort(command.getCommandId());
      Object[] args = command.getParameters();
      byte numArgs = (byte) (args == null ? 0 : args.length);
      output.writeByte(numArgs);
      for (int i = 0; i < numArgs; i++) {
         output.writeObject(args[i]);
      }
   }

   public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
      ReplicableCommand command = (ReplicableCommand)createExternal(input);
      short methodId = input.readShort();
      byte numArgs = input.readByte();
      Object[] args = null;
      if (numArgs > 0) {
         args = new Object[numArgs];
         for (int i = 0; i < numArgs; i++) args[i] = input.readObject();
      }
      command.setParameters(methodId, args);
      return command;
   }
   
   protected void writeClass(Marshaller output, Class<?> subjectType) throws IOException {
      classExt.writeClass(output, subjectType);
   }
   
   protected Object createExternal(Unmarshaller input) throws IOException, ClassNotFoundException {
      try {
         Class<?> subjectType = classExt.readClass(input);
         ReplicableCommand command = (ReplicableCommand) Util.getInstance(subjectType);
         return command;
      } catch (Exception e) {
         throw new CacheException("Unable to create new instance of ReplicableCommand", e);
      }
   }
   
   public void setClassExternalizer(ClassExternalizer classExt) {
      this.classExt = classExt;
   }
}