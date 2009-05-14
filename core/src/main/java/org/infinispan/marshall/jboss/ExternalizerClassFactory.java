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
import org.infinispan.atomic.AtomicHashMap;
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
import org.infinispan.marshall.jboss.externalizers.*;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.ExtendedResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.FastCopyHashMap;
import org.infinispan.util.Util;
import org.jboss.marshalling.ClassExternalizerFactory;
import org.jboss.marshalling.Externalizer;

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
 * CustomExternalizerFactory.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class ExternalizerClassFactory implements ClassExternalizerFactory {
   private static final Map<String, String> EXTERNALIZERS = new HashMap<String, String>();

   static {
      EXTERNALIZERS.put(GlobalTransaction.class.getName(), GlobalTransactionExternalizer.class.getName());
      EXTERNALIZERS.put(JGroupsAddress.class.getName(), JGroupsAddressExternalizer.class.getName());
      EXTERNALIZERS.put(ArrayList.class.getName(), ArrayListExternalizer.class.getName());
      EXTERNALIZERS.put(LinkedList.class.getName(), LinkedListExternalizer.class.getName());
      EXTERNALIZERS.put(HashMap.class.getName(), MapExternalizer.class.getName());
      EXTERNALIZERS.put(TreeMap.class.getName(), MapExternalizer.class.getName());
      EXTERNALIZERS.put(HashSet.class.getName(), SetExternalizer.class.getName());
      EXTERNALIZERS.put(TreeSet.class.getName(), SetExternalizer.class.getName());
      EXTERNALIZERS.put("org.infinispan.util.Immutables$ImmutableMapWrapper", ImmutableMapExternalizer.class.getName());
      EXTERNALIZERS.put(MarshalledValue.class.getName(), MarshalledValueExternalizer.class.getName());
      EXTERNALIZERS.put(FastCopyHashMap.class.getName(), MapExternalizer.class.getName());
      EXTERNALIZERS.put("java.util.Collections$SingletonList", SingletonListExternalizer.class.getName());
      EXTERNALIZERS.put("org.infinispan.transaction.TransactionLog$LogEntry", TransactionLogExternalizer.class.getName());
      EXTERNALIZERS.put(ExtendedResponse.class.getName(), ExtendedResponseExternalizer.class.getName());
      EXTERNALIZERS.put(SuccessfulResponse.class.getName(), SuccessfulResponseExternalizer.class.getName());
      EXTERNALIZERS.put(UnsuccessfulResponse.class.getName(), UnsuccessfulResponseExternalizer.class.getName());
      EXTERNALIZERS.put(ExceptionResponse.class.getName(), ExceptionResponseExternalizer.class.getName());
      EXTERNALIZERS.put(AtomicHashMap.class.getName(), DeltaAwareExternalizer.class.getName());

      EXTERNALIZERS.put(StateTransferControlCommand.class.getName(), StateTransferControlCommandExternalizer.class.getName());
      EXTERNALIZERS.put(ClusteredGetCommand.class.getName(), ReplicableCommandExternalizer.class.getName());
      EXTERNALIZERS.put(MultipleRpcCommand.class.getName(), ReplicableCommandExternalizer.class.getName());
      EXTERNALIZERS.put(SingleRpcCommand.class.getName(), ReplicableCommandExternalizer.class.getName());
      EXTERNALIZERS.put(GetKeyValueCommand.class.getName(), ReplicableCommandExternalizer.class.getName());
      EXTERNALIZERS.put(PutKeyValueCommand.class.getName(), ReplicableCommandExternalizer.class.getName());
      EXTERNALIZERS.put(RemoveCommand.class.getName(), ReplicableCommandExternalizer.class.getName());
      EXTERNALIZERS.put(InvalidateCommand.class.getName(), ReplicableCommandExternalizer.class.getName());
      EXTERNALIZERS.put(ReplaceCommand.class.getName(), ReplicableCommandExternalizer.class.getName());
      EXTERNALIZERS.put(ClearCommand.class.getName(), ReplicableCommandExternalizer.class.getName());
      EXTERNALIZERS.put(PutMapCommand.class.getName(), ReplicableCommandExternalizer.class.getName());
      EXTERNALIZERS.put(PrepareCommand.class.getName(), ReplicableCommandExternalizer.class.getName());
      EXTERNALIZERS.put(CommitCommand.class.getName(), ReplicableCommandExternalizer.class.getName());
      EXTERNALIZERS.put(RollbackCommand.class.getName(), ReplicableCommandExternalizer.class.getName());

      EXTERNALIZERS.put(ImmortalCacheEntry.class.getName(), InternalCachedEntryExternalizer.class.getName());
      EXTERNALIZERS.put(MortalCacheEntry.class.getName(), InternalCachedEntryExternalizer.class.getName());
      EXTERNALIZERS.put(TransientCacheEntry.class.getName(), InternalCachedEntryExternalizer.class.getName());
      EXTERNALIZERS.put(TransientMortalCacheEntry.class.getName(), InternalCachedEntryExternalizer.class.getName());

      EXTERNALIZERS.put(InvalidateL1Command.class.getName(), ReplicableCommandExternalizer.class.getName());
   }

   private final Map<Class<?>, Externalizer> externalizers = new WeakHashMap<Class<?>, Externalizer>();
   private final Transport transport;
   private final CustomObjectTable objectTable;

   public ExternalizerClassFactory(Transport transport, CustomObjectTable objectTable) {
      this.transport = transport;
      this.objectTable = objectTable;
   }

   public void init() {
      try {
         for (Map.Entry<String, String> entry : EXTERNALIZERS.entrySet()) {
            Class typeClazz = Util.loadClass(entry.getKey());
            Externalizer ext = (Externalizer) Util.getInstance(entry.getValue());
            if (ext instanceof StateTransferControlCommandExternalizer) {
               ((StateTransferControlCommandExternalizer) ext).init(transport);
            }
            externalizers.put(typeClazz, ext);
            objectTable.add(ext);
         }
      } catch (IOException e) {
         throw new CacheException("Unable to open load magicnumbers.properties", e);
      } catch (ClassNotFoundException e) {
         throw new CacheException("Unable to load one of the classes defined in the magicnumbers.properties", e);
      } catch (Exception e) {
         throw new CacheException("Unable to instantiate Externalizer class", e);
      }
   }

   public void stop() {
      externalizers.clear();
   }

   public Externalizer getExternalizer(Class<?> clazz) {
      return externalizers.get(clazz);
   }
}
