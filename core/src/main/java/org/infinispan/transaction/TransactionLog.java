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
package org.infinispan.transaction;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Logs transactions and writes for Non-Blocking State Transfer
 *
 * @author Jason T. Greene
 */
@Scope(Scopes.NAMED_CACHE)
public class TransactionLog {
   private Map<GlobalTransaction, PrepareCommand> pendingPrepares;
   private final BlockingQueue<LogEntry> entries = new LinkedBlockingQueue<LogEntry>();
   private AtomicBoolean active = new AtomicBoolean();
   private Configuration configuration;

   @Inject
   private void init(Configuration configuration) {
      this.configuration = configuration;
   }

   @Start
   private void start() {
     pendingPrepares = new ConcurrentHashMap<GlobalTransaction, PrepareCommand>(configuration.getConcurrencyLevel());
   }
   
   
   public static class LogEntry {
      private final GlobalTransaction transaction;
      private final WriteCommand[] modifications;

      public LogEntry(GlobalTransaction transaction, WriteCommand... modifications) {
         this.transaction = transaction;
         this.modifications = modifications;
      }

      public GlobalTransaction getTransaction() {
         return transaction;
      }

      public WriteCommand[] getModifications() {
         return modifications;
      }

      public static class Externalizer extends AbstractExternalizer<LogEntry> {
         @Override
         public void writeObject(ObjectOutput output, TransactionLog.LogEntry le) throws IOException {
            output.writeObject(le.transaction);
            WriteCommand[] cmds = le.modifications;
            UnsignedNumeric.writeUnsignedInt(output, cmds.length);
            for (WriteCommand c : cmds) output.writeObject(c);
         }

         @Override
         public TransactionLog.LogEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            GlobalTransaction gtx = (GlobalTransaction) input.readObject();
            int numCommands = UnsignedNumeric.readUnsignedInt(input);
            WriteCommand[] cmds = new WriteCommand[numCommands];
            for (int i = 0; i < numCommands; i++) cmds[i] = (WriteCommand) input.readObject();
            return new TransactionLog.LogEntry(gtx, cmds);
         }

         @Override
         public Integer getId() {
            return Ids.TRANSACTION_LOG_ENTRY;
         }

         @Override
         public Set<Class<? extends LogEntry>> getTypeClasses() {
            return Util.<Class<? extends LogEntry>>asSet(LogEntry.class);
         }
      }
   }

   private static final Log log = LogFactory.getLog(TransactionLog.class);

   public void logPrepare(PrepareCommand command) {
      pendingPrepares.put(command.getGlobalTransaction(), command);
   }

   public void logCommit(GlobalTransaction gtx) {
      PrepareCommand command = pendingPrepares.remove(gtx);
      // it is perfectly normal for a prepare not to be logged for this gtx, for example if a transaction did not
      // modify anything, then beforeCompletion() is not invoked and logPrepare() will not be called to register the
      // prepare.
      if (command != null && isActive()) addEntry(gtx, command.getModifications());
   }

   private void addEntry(GlobalTransaction gtx, WriteCommand... commands) {
      LogEntry entry = new LogEntry(gtx, commands);
      boolean success = false;
      while (!success) {
         try {
            if (log.isTraceEnabled()) log.tracef("Added commit entry to tx log %s", entry);

            entries.put(entry);
            success = true;
         }
         catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
   }

   public final void logOnePhaseCommit(GlobalTransaction gtx, WriteCommand[] modifications) {
      // Just in case...
      if (gtx != null) pendingPrepares.remove(gtx);
      if (isActive() && modifications != null && modifications.length > 0)
         addEntry(gtx, modifications);
   }

   public final void logNoTxWrite(WriteCommand write) {
      if (isActive()) addEntry(null, write);
   }

   public void rollback(GlobalTransaction gtx) {
      pendingPrepares.remove(gtx);
   }

   public final boolean isActive() {
      return active.get();
   }

   public final boolean activate() {
      return active.compareAndSet(false, true);
   }

   public final void deactivate() {
      active.set(false);
      if (!entries.isEmpty())
         log.unprocessedTxLogEntries(entries.size());
      entries.clear();
   }

   public final int size() {
      return entries.size();
   }

   public void writeCommitLog(StreamingMarshaller marshaller, ObjectOutput out) throws Exception {
      List<LogEntry> buffer = new ArrayList<LogEntry>(10);

      while (entries.drainTo(buffer, 10) > 0) {
         for (LogEntry entry : buffer)
            marshaller.objectToObjectStream(entry, out);

         buffer.clear();
      }
   }

   public void writePendingPrepares(StreamingMarshaller marshaller, ObjectOutput out) throws Exception {
      if (log.isTraceEnabled()) log.tracef("Writing %s pending prepares to the stream", pendingPrepares.size());
      for (PrepareCommand entry : pendingPrepares.values()) marshaller.objectToObjectStream(entry, out);
   }

   public boolean hasPendingPrepare(PrepareCommand command) {
      return pendingPrepares.containsKey(command.getGlobalTransaction());
   }
}
