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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

import net.jcip.annotations.Immutable;

import org.infinispan.CacheException;
import org.infinispan.util.Util;
import org.jboss.marshalling.ClassTable;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;

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
      MAGIC_NUMBERS.put("org.infinispan.transaction.GlobalTransaction", 1);
      MAGIC_NUMBERS.put("org.infinispan.remoting.transport.jgroups.JGroupsAddress", 2);
      MAGIC_NUMBERS.put("java.util.ArrayList", 3);
      MAGIC_NUMBERS.put("java.util.LinkedList", 8);
      MAGIC_NUMBERS.put("java.util.HashMap", 9);
      MAGIC_NUMBERS.put("java.util.TreeMap", 10);
      MAGIC_NUMBERS.put("java.util.HashSet", 11);
      MAGIC_NUMBERS.put("java.util.TreeSet", 12);
      MAGIC_NUMBERS.put("org.infinispan.util.Immutables$ImmutableMapWrapper", 14);
      MAGIC_NUMBERS.put("org.infinispan.marshall.MarshalledValue", 15);
      MAGIC_NUMBERS.put("org.infinispan.util.FastCopyHashMap", 16);
      MAGIC_NUMBERS.put("java.util.Collections$SingletonList", 23);
      MAGIC_NUMBERS.put("org.infinispan.transaction.TransactionLog$LogEntry", 25);
      MAGIC_NUMBERS.put("org.infinispan.remoting.transport.jgroups.ExtendedResponse", 28);
      MAGIC_NUMBERS.put("org.infinispan.atomic.AtomicHashMap", 29);

      MAGIC_NUMBERS.put("org.infinispan.commands.control.StateTransferControlCommand", 30);
      MAGIC_NUMBERS.put("org.infinispan.commands.remote.ClusteredGetCommand", 31);
      MAGIC_NUMBERS.put("org.infinispan.commands.remote.MultipleRpcCommand", 32);
      MAGIC_NUMBERS.put("org.infinispan.commands.remote.SingleRpcCommand", 33);
      MAGIC_NUMBERS.put("org.infinispan.commands.read.GetKeyValueCommand", 34);
      MAGIC_NUMBERS.put("org.infinispan.commands.write.PutKeyValueCommand", 35);
      MAGIC_NUMBERS.put("org.infinispan.commands.write.RemoveCommand", 36);
      MAGIC_NUMBERS.put("org.infinispan.commands.write.InvalidateCommand", 38);
      MAGIC_NUMBERS.put("org.infinispan.commands.write.ReplaceCommand", 39);
      MAGIC_NUMBERS.put("org.infinispan.commands.write.ClearCommand", 40);
      MAGIC_NUMBERS.put("org.infinispan.commands.write.PutMapCommand", 41);
      MAGIC_NUMBERS.put("org.infinispan.commands.tx.PrepareCommand", 42);
      MAGIC_NUMBERS.put("org.infinispan.commands.tx.CommitCommand", 43);
      MAGIC_NUMBERS.put("org.infinispan.commands.tx.RollbackCommand", 44);

      MAGIC_NUMBERS.put("org.infinispan.container.entries.ImmortalCacheEntry", 45);
      MAGIC_NUMBERS.put("org.infinispan.container.entries.MortalCacheEntry", 46);
      MAGIC_NUMBERS.put("org.infinispan.container.entries.TransientCacheEntry", 47);
      MAGIC_NUMBERS.put("org.infinispan.container.entries.TransientMortalCacheEntry", 48);      
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
