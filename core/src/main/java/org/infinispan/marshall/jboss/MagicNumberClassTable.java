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
package org.infinispan.marshall.jboss;

import net.jcip.annotations.Immutable;
import org.infinispan.CacheException;
import org.infinispan.atomic.atomichashmap.AtomicHashMap;
import org.infinispan.commands.control.StateTransferControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.ExtendedResponse;
import org.infinispan.remoting.responses.RequestIgnoredResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.FastCopyHashMap;
import org.infinispan.util.Util;
import org.jboss.marshalling.ClassTable;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.WeakHashMap;

/**
 * MagicNumberClassTable.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class MagicNumberClassTable implements ClassTable {
   private static final Map<String, Integer> MAGIC_NUMBERS = new WeakHashMap<String, Integer>();

   static {
      MAGIC_NUMBERS.put(GlobalTransaction.class.getName(), 1);
      MAGIC_NUMBERS.put(JGroupsAddress.class.getName(), 2);
      MAGIC_NUMBERS.put(ArrayList.class.getName(), 3);
      MAGIC_NUMBERS.put(LinkedList.class.getName(), 4);
      MAGIC_NUMBERS.put(HashMap.class.getName(), 5);
      MAGIC_NUMBERS.put(TreeMap.class.getName(), 6);
      MAGIC_NUMBERS.put(HashSet.class.getName(), 7);
      MAGIC_NUMBERS.put(TreeSet.class.getName(), 8);
      MAGIC_NUMBERS.put("org.infinispan.util.Immutables$ImmutableMapWrapper", 9);
      MAGIC_NUMBERS.put(MarshalledValue.class.getName(), 10);
      MAGIC_NUMBERS.put(FastCopyHashMap.class.getName(), 11);
      MAGIC_NUMBERS.put("java.util.Collections$SingletonList", 12);
      MAGIC_NUMBERS.put("org.infinispan.transaction.TransactionLog$LogEntry", 13);

      MAGIC_NUMBERS.put(RequestIgnoredResponse.class.getName(), 14);
      MAGIC_NUMBERS.put(ExtendedResponse.class.getName(), 15);
      MAGIC_NUMBERS.put(ExceptionResponse.class.getName(), 16);
      MAGIC_NUMBERS.put(SuccessfulResponse.class.getName(), 17);
      MAGIC_NUMBERS.put(UnsuccessfulResponse.class.getName(), 18);


      MAGIC_NUMBERS.put(AtomicHashMap.class.getName(), 19);
      MAGIC_NUMBERS.put(StateTransferControlCommand.class.getName(), 20);
      MAGIC_NUMBERS.put(ClusteredGetCommand.class.getName(), 21);
      MAGIC_NUMBERS.put(MultipleRpcCommand.class.getName(), 22);
      MAGIC_NUMBERS.put(SingleRpcCommand.class.getName(), 23);
      MAGIC_NUMBERS.put(GetKeyValueCommand.class.getName(), 24);
      MAGIC_NUMBERS.put(PutKeyValueCommand.class.getName(), 25);
      MAGIC_NUMBERS.put(RemoveCommand.class.getName(), 26);
      MAGIC_NUMBERS.put(InvalidateCommand.class.getName(), 27);
      MAGIC_NUMBERS.put(ReplaceCommand.class.getName(), 28);
      MAGIC_NUMBERS.put(ClearCommand.class.getName(), 29);
      MAGIC_NUMBERS.put(PutMapCommand.class.getName(), 30);
      MAGIC_NUMBERS.put(PrepareCommand.class.getName(), 31);
      MAGIC_NUMBERS.put(CommitCommand.class.getName(), 32);
      MAGIC_NUMBERS.put(RollbackCommand.class.getName(), 33);

      MAGIC_NUMBERS.put(ImmortalCacheEntry.class.getName(), 34);
      MAGIC_NUMBERS.put(MortalCacheEntry.class.getName(), 35);
      MAGIC_NUMBERS.put(TransientCacheEntry.class.getName(), 36);
      MAGIC_NUMBERS.put(TransientMortalCacheEntry.class.getName(), 37);
      
      MAGIC_NUMBERS.put(InvalidateL1Command.class.getName(), 38);
   }

   private final Map<Class<?>, Writer> writers = new WeakHashMap<Class<?>, Writer>();
   private final Map<Byte, Class<?>> classes = new HashMap<Byte, Class<?>>();

   public void init() {
      try {
         for (Map.Entry<String, Integer> entry : MAGIC_NUMBERS.entrySet()) {
            Class clazz = Util.loadClass(entry.getKey());
            Byte magicNumber = entry.getValue().byteValue();
            Writer writer = createWriter(magicNumber);
            writers.put(clazz, writer);
            classes.put(magicNumber, clazz);
         }
      } catch (ClassNotFoundException e) {
         throw new CacheException("Unable to load one of the classes defined in the magicnumbers.properties", e);
      }
   }

   public void stop() {
      writers.clear();
      classes.clear();
   }

   public Writer getClassWriter(Class<?> clazz) throws IOException {
      return writers.get(clazz);
   }

   public Class<?> readClass(Unmarshaller unmarshaller) throws IOException, ClassNotFoundException {
      byte magicNumber = unmarshaller.readByte();
      return classes.get(magicNumber);
   }

   protected Writer createWriter(byte magicNumber) {
      return new MagicNumberWriter(magicNumber);
   }

   @Immutable
   static class MagicNumberWriter implements Writer {
      private final byte magicNumber;

      MagicNumberWriter(byte magicNumber) {
         this.magicNumber = magicNumber;
      }

      public void writeClass(Marshaller marshaller, Class<?> clazz) throws IOException {
         marshaller.writeByte(magicNumber);
      }
   }
}
