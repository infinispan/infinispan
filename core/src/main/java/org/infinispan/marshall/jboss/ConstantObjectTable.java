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
import org.infinispan.marshall.Externalizer;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.marshall.exts.ArrayListExternalizer;
import org.infinispan.marshall.exts.BucketExternalizer;
import org.infinispan.marshall.exts.DeltaAwareExternalizer;
import org.infinispan.marshall.exts.ExceptionResponseExternalizer;
import org.infinispan.marshall.exts.ExtendedResponseExternalizer;
import org.infinispan.marshall.exts.GlobalTransactionExternalizer;
import org.infinispan.marshall.exts.ImmortalCacheEntryExternalizer;
import org.infinispan.marshall.exts.ImmortalCacheValueExternalizer;
import org.infinispan.marshall.exts.ImmutableMapExternalizer;
import org.infinispan.marshall.exts.JGroupsAddressExternalizer;
import org.infinispan.marshall.exts.LinkedListExternalizer;
import org.infinispan.marshall.exts.MapExternalizer;
import org.infinispan.marshall.exts.MarshalledValueExternalizer;
import org.infinispan.marshall.exts.MortalCacheEntryExternalizer;
import org.infinispan.marshall.exts.MortalCacheValueExternalizer;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.marshall.exts.SetExternalizer;
import org.infinispan.marshall.exts.SingletonListExternalizer;
import org.infinispan.marshall.exts.SuccessfulResponseExternalizer;
import org.infinispan.marshall.exts.TransactionLogExternalizer;
import org.infinispan.marshall.exts.TransientCacheEntryExternalizer;
import org.infinispan.marshall.exts.TransientCacheValueExternalizer;
import org.infinispan.marshall.exts.TransientMortalCacheEntryExternalizer;
import org.infinispan.marshall.exts.TransientMortalCacheValueExternalizer;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.ExtendedResponse;
import org.infinispan.remoting.responses.RequestIgnoredResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.FastCopyHashMap;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
   private static final Log log = LogFactory.getLog(ConstantObjectTable.class);
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
      
      EXTERNALIZERS.put("org.infinispan.tree.NodeKey", "org.infinispan.tree.marshall.exts.NodeKeyExternalizer");
      EXTERNALIZERS.put("org.infinispan.tree.Fqn", "org.infinispan.tree.marshall.exts.FqnExternalizer");
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
   private final Map<Class<?>, ExternalizerAdapter> writers = new IdentityHashMap<Class<?>, ExternalizerAdapter>(CAPACITY);

   private byte index;

   public void init(RemoteCommandFactory cmdFactory, org.infinispan.marshall.Marshaller ispnMarshaller) {
      // Init singletons
      objects.add(RequestIgnoredResponse.INSTANCE);
      writers.put(RequestIgnoredResponse.class, new ExternalizerAdapter(index++, new InstanceWriter()));
      objects.add(UnsuccessfulResponse.INSTANCE);
      writers.put(UnsuccessfulResponse.class, new ExternalizerAdapter(index++, new InstanceWriter()));
      
      try {
         for (Map.Entry<String, String> entry : EXTERNALIZERS.entrySet()) {
            try {
               Class typeClazz = Util.loadClass(entry.getKey());
               Externalizer delegate = (Externalizer) Util.getInstance(entry.getValue());
               if (delegate instanceof ReplicableCommandExternalizer) {
                  ((ReplicableCommandExternalizer) delegate).init(cmdFactory);
               }
               if (delegate instanceof MarshalledValueExternalizer) {
                  ((MarshalledValueExternalizer) delegate).init(ispnMarshaller);
               }
               ExternalizerAdapter rwrt = new ExternalizerAdapter(index++, delegate);
               objects.add(rwrt);
               writers.put(typeClazz, rwrt);               
            } catch (ClassNotFoundException e) {
               log.debug("Unable to load class" + e.getMessage());
            }
         }
         
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
      if (o instanceof ExternalizerAdapter) {
         return ((ExternalizerAdapter) o).readObject(unmarshaller);
      }
      return o;
   }
   
   static class InstanceWriter implements Externalizer {
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         // no-op
      }
      
      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         throw new CacheException("Read on constant instances shouldn't be called");
      }
   }
   
   class ExternalizerAdapter implements Writer {
      final byte id;
      final Externalizer externalizer;
      
      ExternalizerAdapter(byte objectId, Externalizer externalizer) {
         this.id = objectId;
         this.externalizer = externalizer;
      }
      
      public Object readObject(Unmarshaller unmarshaller) throws IOException, ClassNotFoundException {
         return externalizer.readObject(new ObjectInputAdapter(unmarshaller));
      }

      public void writeObject(Marshaller marshaller, Object object) throws IOException {
         marshaller.write(id);
         externalizer.writeObject(new ObjectOutputAdapter(marshaller), object);
      }
   }

   static class ObjectInputAdapter implements ObjectInput {
      final ObjectInput input;
      
      ObjectInputAdapter(ObjectInput input) {
         this.input = input;
      }

      public int available() throws IOException {
         return input.available();
      }

      public void close() throws IOException {
         input.close();
      }

      public int read() throws IOException {
         return input.read();
      }

      public int read(byte[] b) throws IOException {
         return input.read(b);
      }

      public int read(byte[] b, int off, int len) throws IOException {
         return input.read(b, off, len);
      }

      public Object readObject() throws ClassNotFoundException, IOException {
         return input.readObject();
      }

      public long skip(long n) throws IOException {
         return input.skip(n);
      }

      public boolean readBoolean() throws IOException {
         return input.readBoolean();
      }

      public byte readByte() throws IOException {
         return input.readByte();
      }

      public char readChar() throws IOException {
         return input.readChar();
      }

      public double readDouble() throws IOException {
         return input.readDouble();
      }

      public float readFloat() throws IOException {
         return input.readFloat();
      }

      public void readFully(byte[] b) throws IOException {
         input.readFully(b);
      }

      public void readFully(byte[] b, int off, int len) throws IOException {
         input.readFully(b, off, len);
      }

      public int readInt() throws IOException {
         return input.readInt();
      }

      public String readLine() throws IOException {
         return input.readLine();
      }

      public long readLong() throws IOException {
         return input.readLong();
      }

      public short readShort() throws IOException {
         return input.readShort();
      }

      public String readUTF() throws IOException {
         return input.readUTF();
      }

      public int readUnsignedByte() throws IOException {
         return input.readUnsignedByte();
      }

      public int readUnsignedShort() throws IOException {
         return input.readUnsignedShort();
      }

      public int skipBytes(int n) throws IOException {
         return input.skipBytes(n);
      }
   }
   
   static class ObjectOutputAdapter implements ObjectOutput {
      final ObjectOutput output;
      
      ObjectOutputAdapter(ObjectOutput output) {
         this.output = output;
      }

      public void close() throws IOException {
         output.close();
      }

      public void flush() throws IOException {
         output.flush();
      }

      public void write(int b) throws IOException {
         output.write((int)b);
      }

      public void write(byte[] b) throws IOException {
         output.write(b);
      }

      public void write(byte[] b, int off, int len) throws IOException {
         output.write(b, off, len);      
      }

      public void writeObject(Object obj) throws IOException {
         output.writeObject(obj);
      }

      public void writeBoolean(boolean v) throws IOException {
         output.writeBoolean(v);
      }

      public void writeByte(int v) throws IOException {
         output.writeByte(v);
      }

      public void writeBytes(String s) throws IOException {
         output.writeBytes(s);
      }

      public void writeChar(int v) throws IOException {
         output.writeChar(v);      
      }

      public void writeChars(String s) throws IOException {
         output.writeChars(s);
      }

      public void writeDouble(double v) throws IOException {
         output.writeDouble(v);
      }

      public void writeFloat(float v) throws IOException {
         output.writeFloat(v);      
      }

      public void writeInt(int v) throws IOException {
         output.writeInt(v);
      }

      public void writeLong(long v) throws IOException {
         output.writeLong(v);      
      }

      public void writeShort(int v) throws IOException {
         output.writeShort(v);
      }

      public void writeUTF(String str) throws IOException {
         output.writeUTF(str);      
      }
   }
}
