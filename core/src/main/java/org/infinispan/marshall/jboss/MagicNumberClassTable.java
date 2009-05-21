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
import org.infinispan.commands.LockControlCommand;
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
import org.infinispan.util.FastCopyHashMap;
import org.infinispan.util.Util;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * MagicNumberClassTable.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class MagicNumberClassTable implements ClassExternalizer {
   private static final List<String> MAGIC_NUMBERS = new ArrayList<String>();

   static {
      MAGIC_NUMBERS.add(HashMap.class.getName());
      MAGIC_NUMBERS.add(TreeMap.class.getName());
      MAGIC_NUMBERS.add(FastCopyHashMap.class.getName());
      
      MAGIC_NUMBERS.add(HashSet.class.getName());
      MAGIC_NUMBERS.add(TreeSet.class.getName());
      
      MAGIC_NUMBERS.add(ClusteredGetCommand.class.getName());
      MAGIC_NUMBERS.add(MultipleRpcCommand.class.getName());
      MAGIC_NUMBERS.add(SingleRpcCommand.class.getName());
      MAGIC_NUMBERS.add(GetKeyValueCommand.class.getName());
      MAGIC_NUMBERS.add(PutKeyValueCommand.class.getName());
      MAGIC_NUMBERS.add(RemoveCommand.class.getName());
      MAGIC_NUMBERS.add(InvalidateCommand.class.getName());
      MAGIC_NUMBERS.add(ReplaceCommand.class.getName());
      MAGIC_NUMBERS.add(ClearCommand.class.getName());
      MAGIC_NUMBERS.add(PutMapCommand.class.getName());
      MAGIC_NUMBERS.add(PrepareCommand.class.getName());
      MAGIC_NUMBERS.add(CommitCommand.class.getName());
      MAGIC_NUMBERS.add(RollbackCommand.class.getName());
      MAGIC_NUMBERS.add(InvalidateL1Command.class.getName());
      MAGIC_NUMBERS.add(LockControlCommand.class.getName());
   }
   
   /** Class to int mapping providing magic number to be written. Do not use 
    * this map for storing user classes. For these, please use weak key based 
    * maps, i.e WeakHashMap */
   private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<Class<?>>();
   /** Contains list of class objects written. When writing, index of each 
    * class object object, or magic number, is written, and when reading, index 
    * is used to find the instance in this list.*/
   private final List<Class<?>> classes = new ArrayList<Class<?>>();
   private byte index;

   public MagicNumberClassTable() {
      try {
         for (String entry : MAGIC_NUMBERS) {
            Class clazz = Util.loadClass(entry);
            numbers.put(clazz, index++);
            classes.add(clazz);
         }
      } catch (ClassNotFoundException e) {
         throw new CacheException("Unable to load one of the classes defined in the magicnumbers.properties", e);
      } catch (Exception e) {
         throw new CacheException("Unable to instantiate Externalizer class", e);
      }
   }

   public void stop() {
      classes.clear();
      numbers.clear();
   }

   public void writeClass(Marshaller marshaller, Class<?> clazz) throws IOException {
      int number = numbers.get(clazz, -1);
      marshaller.writeByte(number);
   }

   public Class<?> readClass(Unmarshaller unmarshaller) throws IOException {
      int magicNumber = unmarshaller.readUnsignedByte();
      return classes.get(magicNumber);
   }
   
}
