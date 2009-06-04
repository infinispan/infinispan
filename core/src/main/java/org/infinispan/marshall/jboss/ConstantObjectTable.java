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
import org.infinispan.commands.LockControlCommand;
import org.infinispan.commands.RemoteCommandFactory;
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
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.marshall.jboss.externalizers.ArrayListExternalizer;
import org.infinispan.marshall.jboss.externalizers.BucketExternalizer;
import org.infinispan.marshall.jboss.externalizers.DeltaAwareExternalizer;
import org.infinispan.marshall.jboss.externalizers.ExceptionResponseExternalizer;
import org.infinispan.marshall.jboss.externalizers.ExtendedResponseExternalizer;
import org.infinispan.marshall.jboss.externalizers.GlobalTransactionExternalizer;
import org.infinispan.marshall.jboss.externalizers.ImmortalCacheEntryExternalizer;
import org.infinispan.marshall.jboss.externalizers.ImmortalCacheValueExternalizer;
import org.infinispan.marshall.jboss.externalizers.ImmutableMapExternalizer;
import org.infinispan.marshall.jboss.externalizers.JGroupsAddressExternalizer;
import org.infinispan.marshall.jboss.externalizers.LinkedListExternalizer;
import org.infinispan.marshall.jboss.externalizers.MapExternalizer;
import org.infinispan.marshall.jboss.externalizers.MarshalledValueExternalizer;
import org.infinispan.marshall.jboss.externalizers.MortalCacheEntryExternalizer;
import org.infinispan.marshall.jboss.externalizers.MortalCacheValueExternalizer;
import org.infinispan.marshall.jboss.externalizers.ReplicableCommandExternalizer;
import org.infinispan.marshall.jboss.externalizers.SetExternalizer;
import org.infinispan.marshall.jboss.externalizers.SingletonListExternalizer;
import org.infinispan.marshall.jboss.externalizers.SuccessfulResponseExternalizer;
import org.infinispan.marshall.jboss.externalizers.TransactionLogExternalizer;
import org.infinispan.marshall.jboss.externalizers.TransientCacheEntryExternalizer;
import org.infinispan.marshall.jboss.externalizers.TransientCacheValueExternalizer;
import org.infinispan.marshall.jboss.externalizers.TransientMortalCacheEntryExternalizer;
import org.infinispan.marshall.jboss.externalizers.TransientMortalCacheValueExternalizer;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.ExtendedResponse;
import org.infinispan.remoting.responses.RequestIgnoredResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.FastCopyHashMap;
import org.infinispan.util.Util;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Constant ObjectTable that marshalls constant instances regardless of whether 
 * these are generic objects such as UnsuccessfulResponse.INSTANCE, or home grown 
 * Externalizer implementations. In both cases, this is a hugely efficient way of 
 * sending around constant singleton objects. 
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public class ConstantObjectTable implements ObjectTable {
   private static final int CAPACITY = 50;
   private static final Map<String, String> EXTERNALIZERS = new HashMap<String, String>(CAPACITY);

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
      EXTERNALIZERS.put(ExceptionResponse.class.getName(), ExceptionResponseExternalizer.class.getName());
      EXTERNALIZERS.put(AtomicHashMap.class.getName(), DeltaAwareExternalizer.class.getName());

      EXTERNALIZERS.put(StateTransferControlCommand.class.getName(), ReplicableCommandExternalizer.class.getName());
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
      EXTERNALIZERS.put(InvalidateL1Command.class.getName(), ReplicableCommandExternalizer.class.getName());
      EXTERNALIZERS.put(LockControlCommand.class.getName(), ReplicableCommandExternalizer.class.getName());

      EXTERNALIZERS.put(ImmortalCacheEntry.class.getName(), ImmortalCacheEntryExternalizer.class.getName());
      EXTERNALIZERS.put(MortalCacheEntry.class.getName(), MortalCacheEntryExternalizer.class.getName());
      EXTERNALIZERS.put(TransientCacheEntry.class.getName(), TransientCacheEntryExternalizer.class.getName());
      EXTERNALIZERS.put(TransientMortalCacheEntry.class.getName(), TransientMortalCacheEntryExternalizer.class.getName());    
      EXTERNALIZERS.put(ImmortalCacheValue.class.getName(), ImmortalCacheValueExternalizer.class.getName());
      EXTERNALIZERS.put(MortalCacheValue.class.getName(), MortalCacheValueExternalizer.class.getName());
      EXTERNALIZERS.put(TransientCacheValue.class.getName(), TransientCacheValueExternalizer.class.getName());
      EXTERNALIZERS.put(TransientMortalCacheValue.class.getName(), TransientMortalCacheValueExternalizer.class.getName());
      
      EXTERNALIZERS.put(Bucket.class.getName(), BucketExternalizer.class.getName());      
   }

   /** Contains list of singleton objects written such as constant objects, 
    * singleton Externalizer implementations...etc. When writing, index of each 
    * object is written, and when reading, index is used to find the instance 
    * in this list.*/
   private final List<Object> objects = new ArrayList<Object>(CAPACITY);
   
   /** Contains mapping of constant instances to their writers and also custom 
    * object externalizer classes to their Externalizer instances. 
    * Do not use this map for storing Externalizer implementations for user 
    * classes. For these, please use weak key based maps, i.e WeakHashMap */
   private final Map<Class<?>, Writer> writers = new IdentityHashMap<Class<?>, Writer>(CAPACITY);

   private byte index;

   public void init(RemoteCommandFactory cmdFactory, org.infinispan.marshall.Marshaller ispnMarshaller) {
      // Init singletons
      objects.add(RequestIgnoredResponse.INSTANCE);
      writers.put(RequestIgnoredResponse.class, new InstanceWriter(index++));
      objects.add(UnsuccessfulResponse.INSTANCE);
      writers.put(UnsuccessfulResponse.class, new InstanceWriter(index++));
      
      try {
         for (Map.Entry<String, String> entry : EXTERNALIZERS.entrySet()) {
            Class typeClazz = Util.loadClass(entry.getKey());
            Externalizer delegate = (Externalizer) Util.getInstance(entry.getValue());
            if (delegate instanceof ReplicableCommandExternalizer) {
               ((ReplicableCommandExternalizer) delegate).init(cmdFactory);
            }
            if (delegate instanceof MarshalledValueExternalizer) {
               ((MarshalledValueExternalizer) delegate).init(ispnMarshaller);
            }
            Externalizer rwrt = new DelegatingReadWriter(index++, delegate);
            objects.add(rwrt);
            writers.put(typeClazz, rwrt);
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
      writers.clear();
      objects.clear();
   }

   public Writer getObjectWriter(Object o) throws IOException {
      return writers.get(o.getClass());
   }

   public Object readObject(Unmarshaller unmarshaller) throws IOException, ClassNotFoundException {
      Object o = objects.get(unmarshaller.readUnsignedByte());
      if (o instanceof Externalizer) {
         return ((Externalizer) o).readObject(unmarshaller);
      }
      return o;
   }
   
   @Immutable
   static class InstanceWriter implements Writer {
      private final byte id;

      InstanceWriter(byte objectId) {
         this.id = objectId;
      }

      public void writeObject(Marshaller marshaller, Object object) throws IOException {
         marshaller.write(id);
      }
   }   
   
   @Immutable
   static class DelegatingReadWriter implements Externalizer {
      private final byte id;
      private final Externalizer delegate;

      DelegatingReadWriter(byte objectId, Externalizer delegate) {
         this.id = objectId;
         this.delegate = delegate;
      }

      public void writeObject(Marshaller marshaller, Object object) throws IOException {
         marshaller.write(id);
         delegate.writeObject(marshaller, object);
      }

      public Object readObject(Unmarshaller unmarshaller) throws IOException, ClassNotFoundException {
         return delegate.readObject(unmarshaller);
      }
   }   
}
