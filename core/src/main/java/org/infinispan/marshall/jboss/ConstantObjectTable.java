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
import org.infinispan.atomic.AtomicHashMapDelta;
import org.infinispan.atomic.ClearOperation;
import org.infinispan.atomic.PutOperation;
import org.infinispan.atomic.RemoveOperation;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.control.RehashControlCommand;
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
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.config.GlobalConfiguration.MarshallablesType;
import org.infinispan.config.MarshallableConfig;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.distribution.ch.NodeTopologyInfo;
import org.infinispan.distribution.ch.TopologyAwareConsistentHash;
import org.infinispan.distribution.ch.UnionConsistentHash;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.marshall.Externalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.marshall.exts.ArrayListExternalizer;
import org.infinispan.marshall.exts.LinkedListExternalizer;
import org.infinispan.marshall.exts.MapExternalizer;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.marshall.exts.SetExternalizer;
import org.infinispan.marshall.exts.SingletonListExternalizer;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.ExtendedResponse;
import org.infinispan.remoting.responses.RequestIgnoredResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteArrayKey;
import org.infinispan.util.FastCopyHashMap;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Constant ObjectTable that marshalls constant instances regardless of whether these are generic objects such as
 * UnsuccessfulResponse.INSTANCE, or home grown Externalizer implementations. In both cases, this is a hugely efficient
 * way of sending around constant singleton objects.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class ConstantObjectTable implements ObjectTable {
   private static final Log log = LogFactory.getLog(ConstantObjectTable.class);
   private static final Map<String, String> JDK_EXTERNALIZERS = new HashMap<String, String>();
   static final Set<String> MARSHALLABLES = new HashSet<String>();

   static {
      JDK_EXTERNALIZERS.put(ArrayList.class.getName(), ArrayListExternalizer.class.getName());
      JDK_EXTERNALIZERS.put(LinkedList.class.getName(), LinkedListExternalizer.class.getName());
      JDK_EXTERNALIZERS.put(HashMap.class.getName(), MapExternalizer.class.getName());
      JDK_EXTERNALIZERS.put(TreeMap.class.getName(), MapExternalizer.class.getName());
      JDK_EXTERNALIZERS.put(HashSet.class.getName(), SetExternalizer.class.getName());
      JDK_EXTERNALIZERS.put(TreeSet.class.getName(), SetExternalizer.class.getName());
      JDK_EXTERNALIZERS.put("java.util.Collections$SingletonList", SingletonListExternalizer.class.getName());

      // TODO Create foreign externalizers in corresponding modules for any non internal classes
      // TODO Change MARSHALLABLES collection into a Class based collection to speed up starup
      MARSHALLABLES.add(GlobalTransaction.class.getName());
      MARSHALLABLES.add(DldGlobalTransaction.class.getName());
      MARSHALLABLES.add(JGroupsAddress.class.getName());
      MARSHALLABLES.add("org.infinispan.util.Immutables$ImmutableMapWrapper");
      MARSHALLABLES.add(MarshalledValue.class.getName());
      MARSHALLABLES.add(FastCopyHashMap.class.getName());

      MARSHALLABLES.add("org.infinispan.transaction.TransactionLog$LogEntry");
      MARSHALLABLES.add(ExtendedResponse.class.getName());
      MARSHALLABLES.add(SuccessfulResponse.class.getName());
      MARSHALLABLES.add(ExceptionResponse.class.getName());
      MARSHALLABLES.add(RequestIgnoredResponse.class.getName());
      MARSHALLABLES.add(UnsuccessfulResponse.class.getName());
      MARSHALLABLES.add(UnsureResponse.class.getName());

      MARSHALLABLES.add(StateTransferControlCommand.class.getName());
      MARSHALLABLES.add(ClusteredGetCommand.class.getName());
      MARSHALLABLES.add(MultipleRpcCommand.class.getName());
      MARSHALLABLES.add(SingleRpcCommand.class.getName());
      MARSHALLABLES.add(GetKeyValueCommand.class.getName());
      MARSHALLABLES.add(PutKeyValueCommand.class.getName());
      MARSHALLABLES.add(RemoveCommand.class.getName());
      MARSHALLABLES.add(InvalidateCommand.class.getName());
      MARSHALLABLES.add(ReplaceCommand.class.getName());
      MARSHALLABLES.add(ClearCommand.class.getName());
      MARSHALLABLES.add(PutMapCommand.class.getName());
      MARSHALLABLES.add(PrepareCommand.class.getName());
      MARSHALLABLES.add(CommitCommand.class.getName());
      MARSHALLABLES.add(RollbackCommand.class.getName());
      MARSHALLABLES.add(InvalidateL1Command.class.getName());
      MARSHALLABLES.add(LockControlCommand.class.getName());
      MARSHALLABLES.add(RehashControlCommand.class.getName());

      MARSHALLABLES.add(ImmortalCacheEntry.class.getName());
      MARSHALLABLES.add(MortalCacheEntry.class.getName());
      MARSHALLABLES.add(TransientCacheEntry.class.getName());
      MARSHALLABLES.add(TransientMortalCacheEntry.class.getName());
      MARSHALLABLES.add(ImmortalCacheValue.class.getName());
      MARSHALLABLES.add(MortalCacheValue.class.getName());
      MARSHALLABLES.add(TransientCacheValue.class.getName());
      MARSHALLABLES.add(TransientMortalCacheValue.class.getName());

      MARSHALLABLES.add(AtomicHashMap.class.getName());
      MARSHALLABLES.add(Bucket.class.getName());
      MARSHALLABLES.add("org.infinispan.tree.NodeKey");
      MARSHALLABLES.add("org.infinispan.tree.Fqn");
      MARSHALLABLES.add(AtomicHashMapDelta.class.getName());
      MARSHALLABLES.add(PutOperation.class.getName());
      MARSHALLABLES.add(RemoveOperation.class.getName());
      MARSHALLABLES.add(ClearOperation.class.getName());
      MARSHALLABLES.add(DefaultConsistentHash.class.getName());
      MARSHALLABLES.add(UnionConsistentHash.class.getName());
      MARSHALLABLES.add(NodeTopologyInfo.class.getName());
      MARSHALLABLES.add(TopologyAwareConsistentHash.class.getName());

      MARSHALLABLES.add("org.infinispan.server.core.CacheValue");
      MARSHALLABLES.add("org.infinispan.server.memcached.MemcachedValue");
      MARSHALLABLES.add(ByteArrayKey.class.getName());
      MARSHALLABLES.add("org.infinispan.server.hotrod.TopologyAddress");
      MARSHALLABLES.add("org.infinispan.server.hotrod.TopologyView");

   }

   /**
    * Contains mapping of classes to their corresponding Externalizer classes via ExternalizerAdapter instances.
    */
   private final Map<Class<?>, ExternalizerAdapter> writers = new IdentityHashMap<Class<?>, ExternalizerAdapter>();

   /**
    * Contains mapping of ids to their corresponding Externalizer classes via ExternalizerAdapter instances.
    * This maps contains mappings for both internal and foreign or user defined externalizers.
    *
    * Internal ids are only allowed to be unsigned bytes (0 to 254). 255 is an special id that signals the
    * arrival of a foreign externalizer id. Foreign externalizers are only allowed to use positive ids that between 0
    * and Integer.MAX_INT. To avoid clashes between foreign and internal ids, foreign ids are transformed into negative
    * values to be stored in this map. This way, we avoid the need of a second map to hold user defined externalizers.
    */
   private final Map<Integer, ExternalizerAdapter> readers = new HashMap<Integer, ExternalizerAdapter>();

   private volatile boolean started;

   public void start(RemoteCommandsFactory cmdFactory, StreamingMarshaller ispnMarshaller, GlobalConfiguration globalCfg) {
      loadJdkExternalizers();
      loadInternalMarshallables(cmdFactory, ispnMarshaller);
      loadForeignMarshallables(globalCfg);

      started = true;

      if (log.isTraceEnabled())
         log.trace("Constant object table was started and contains these externalizer readers: {0}", readers);
   }

   public void stop() {
      writers.clear();
      readers.clear();
      started = false;
      if (log.isTraceEnabled())
         log.trace("Externalizer reader and writer maps have been cleared and constant object table was stopped");
   }

   public Writer getObjectWriter(Object o) throws IOException {
      Class clazz = o.getClass();
      Writer writer = writers.get(clazz);
      if (writer == null && (MARSHALLABLES.contains(clazz.getName()) || JDK_EXTERNALIZERS.containsKey(clazz.getName()))) {
         if (log.isTraceEnabled())
            log.trace("Either the marshaller has stopped or hasn't started. Write externalizers are not propery populated: {0}", writers);

         if (Thread.currentThread().isInterrupted())
            throw new IOException(String.format(
                  "Cache manager is shutting down, so type write externalizer for type=%s cannot be resolved. Interruption being pushed up.",
                  clazz.getName()), new InterruptedException());
         else
            throw new IllegalStateException(String.format(
                  "No write externalizer available for: %s, either marshaller is stopped or has not started up yet.",
                  clazz.getName()));
      }
      return writer;
   }

   public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
      int readerIndex = input.readUnsignedByte();
      if (readerIndex == Ids.MAX_ID) // User defined externalizer
         readerIndex = generateForeignReaderIndex(UnsignedNumeric.readUnsignedInt(input));

      ExternalizerAdapter adapter = readers.get(readerIndex);
      if (adapter == null) {
         if (!started) {
            if (log.isTraceEnabled())
               log.trace("Either the marshaller has stopped or hasn't started. Read externalizers are not propery populated: {0}", readers);

            if (Thread.currentThread().isInterrupted())
               throw new IOException(String.format(
                     "Cache manager is shutting down, so type (id=%d) cannot be resolved. Interruption being pushed up.",
                     readerIndex), new InterruptedException());
            else
               throw new CacheException(String.format(
                     "Cache manager is either starting up or shutting down but it's not interrupted, so type (id=%d) cannot be resolved.",
                     readerIndex));
         } else {
            if (log.isTraceEnabled()) {
               log.trace("Unknown type. Input stream has {0} to read", input.available());
               log.trace("Check contents of read externalizers: {0}", readers);
            }

            throw new CacheException(String.format(
                  "Type of data read is unknown. Id=%d is not amongst known reader indexes.",
                  readerIndex));
         }
      }

      return adapter.readObject(input);
   }

   private void loadJdkExternalizers() {
      for (Map.Entry<String, String> entry : JDK_EXTERNALIZERS.entrySet()) {
         try {
            Class typeClass = Util.loadClassStrict(entry.getKey());
            Externalizer ext = (Externalizer) Util.getInstanceStrict(entry.getValue());
            Marshallable marshallable = ReflectionUtil.getAnnotation(ext.getClass(), Marshallable.class);
            int id = checkInternalIdLimit(marshallable.id(), ext);
            updateExtReadersWriters(new ExternalizerAdapter(id, ext), typeClass);
         } catch (Exception e) {
            if (log.isDebugEnabled()) {
               log.debug("Unable to load class {0}", e.getMessage());
            }
         }
      }
   }

   private void loadInternalMarshallables(RemoteCommandsFactory cmdFactory, StreamingMarshaller ispnMarshaller) {
      for (String marshallableClass : MARSHALLABLES) {
         try {
            Class typeClass = Util.loadClassStrict(marshallableClass);
            Marshallable marshallable = ReflectionUtil.getAnnotation(typeClass, Marshallable.class);
            if (marshallable != null && !marshallable.externalizer().equals(Externalizer.class)) {
               Externalizer ext = Util.getInstance(marshallable.externalizer());

               if (ext instanceof ReplicableCommandExternalizer)
                  ((ReplicableCommandExternalizer) ext).inject(cmdFactory);

               if (ext instanceof MarshalledValue.Externalizer)
                  ((MarshalledValue.Externalizer) ext).inject(ispnMarshaller);

               int id = checkInternalIdLimit(marshallable.id(), ext);
               updateExtReadersWriters(new ExternalizerAdapter(id, ext), typeClass);
            }
         } catch (ClassNotFoundException e) {
            if (!marshallableClass.startsWith("org.infinispan")) {
               if (log.isDebugEnabled()) log.debug("Unable to load class {0}", e.getMessage());
            }
         }
      }
   }

   private void loadForeignMarshallables(GlobalConfiguration globalCfg) {
      if (log.isTraceEnabled())
         log.trace("Loading user defined externalizers");
      MarshallablesType marshallables = globalCfg.getMarshallableTypes();
      List<MarshallableConfig> configs = marshallables.getMarshallableConfigs();
      for (MarshallableConfig config : configs) {
         Class typeClass = Util.loadClass(config.getTypeClass());
         Externalizer ext = (Externalizer) Util.getInstance(config.getExternalizerClass());
         int id = checkForeignIdLimit(config.getId(), ext);
         updateExtReadersWriters(new ForeignExternalizerAdapter(id, ext), typeClass, generateForeignReaderIndex(id));
      }
   }

   private void updateExtReadersWriters(ExternalizerAdapter adapter, Class typeClass) {
      updateExtReadersWriters(adapter, typeClass, adapter.id);
   }

   private void updateExtReadersWriters(ExternalizerAdapter adapter, Class typeClass, int readerIndex) {
      writers.put(typeClass, adapter);
      ExternalizerAdapter prev = readers.put(readerIndex, adapter);
      if (prev != null)
         throw new CacheException(String.format(
               "Duplicate id found! id=%d in %s is shared by another marshallable class (%s). Reader index is %d",
               adapter.id, adapter.externalizer.getClass().getName(), prev.externalizer.getClass().getName(), readerIndex));

      if (log.isTraceEnabled())
         log.trace("Loaded externalizer {0} for {1} with id {2} and reader index {3}",
                   adapter.externalizer.getClass().getName(), typeClass, adapter.id, readerIndex);

   }

   private int checkInternalIdLimit(int id, Externalizer ext) {
      if (id >= Ids.MAX_ID)
         throw new CacheException(String.format(
               "Internal %s externalizer is using an id(%d) that exceeed the limit. It needs to be smaller than %d",
               ext, id, Ids.MAX_ID));
      return id;
   }

   private int checkForeignIdLimit(int id, Externalizer ext) {
      if (id < 0)
         throw new CacheException(String.format(
               "Foreign %s externalizer is using a negative id(%d). Only positive id values are allowed.",
               ext, id));
      return id;
   }

   private int generateForeignReaderIndex(int foreignId) {
      return 0x80000000 | foreignId;
   }

   static class ExternalizerAdapter implements Writer {
      final int id;
      final Externalizer externalizer;

      ExternalizerAdapter(int id, Externalizer externalizer) {
         this.id = id;
         this.externalizer = externalizer;
      }

      public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
         return externalizer.readObject(input);
      }

      public void writeObject(Marshaller output, Object object) throws IOException {
         output.write(id);
         externalizer.writeObject(output, object);
      }

      @Override
      public String toString() {
         // Each adapter is represented by the externalizer it delegates to, so just return the class name
         return externalizer.getClass().getName();
      }
   }

   static class ForeignExternalizerAdapter extends ExternalizerAdapter {
      final int foreignId;

      ForeignExternalizerAdapter(int foreignId, Externalizer externalizer) {
         super(Ids.MAX_ID, externalizer);
         this.foreignId = foreignId;
      }

      @Override
      public void writeObject(Marshaller output, Object object) throws IOException {
         output.write(id);
         // Write as an unsigned, variable length, integer to safe space
         UnsignedNumeric.writeUnsignedInt(output, foreignId);
         externalizer.writeObject(output, object);
      }
   }
}
