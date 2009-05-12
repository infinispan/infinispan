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

import net.jcip.annotations.Immutable;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.marshall.jboss.MarshallUtil;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.TransactionLog;
import org.jboss.marshalling.Creator;
import org.jboss.marshalling.Externalizer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * TransactionLogExternalizer.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class TransactionLogExternalizer implements Externalizer {

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -7341096933735222157L;

   public void writeExternal(Object subject, ObjectOutput output) throws IOException {
      TransactionLog.LogEntry le = (TransactionLog.LogEntry) subject;
      output.writeObject(le.getTransaction());
      WriteCommand[] cmds = le.getModifications();
      MarshallUtil.writeUnsignedInt(output, cmds.length);
      for (WriteCommand c : cmds)
         output.writeObject(c);
   }

   public Object createExternal(Class<?> subjectType, ObjectInput input, Creator defaultCreator)
         throws IOException, ClassNotFoundException {
      GlobalTransaction gtx = (GlobalTransaction) input.readObject();
      int numCommands = MarshallUtil.readUnsignedInt(input);
      WriteCommand[] cmds = new WriteCommand[numCommands];
      for (int i = 0; i < numCommands; i++) cmds[i] = (WriteCommand) input.readObject();
      return new TransactionLog.LogEntry(gtx, cmds);
   }

   public void readExternal(Object subject, ObjectInput input) throws IOException,
                                                                      ClassNotFoundException {
      // No-op since the initialisation the creation and read happens during the create phase.
   }
}
