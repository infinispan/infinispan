/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.marshall.jboss;

import org.infinispan.CacheException;
import org.infinispan.atomic.AtomicHashMap;
import org.infinispan.atomic.AtomicHashMapDelta;
import org.infinispan.atomic.ClearOperation;
import org.infinispan.atomic.PutOperation;
import org.infinispan.atomic.RemoveOperation;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commons.hash.MurmurHash2;
import org.infinispan.commons.hash.MurmurHash2Compat;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.container.entries.versioned.VersionedImmortalCacheEntry;
import org.infinispan.container.entries.versioned.VersionedImmortalCacheValue;
import org.infinispan.container.entries.versioned.VersionedMortalCacheEntry;
import org.infinispan.container.entries.versioned.VersionedMortalCacheValue;
import org.infinispan.container.entries.versioned.VersionedTransientCacheEntry;
import org.infinispan.container.entries.versioned.VersionedTransientCacheValue;
import org.infinispan.container.entries.versioned.VersionedTransientMortalCacheEntry;
import org.infinispan.container.entries.versioned.VersionedTransientMortalCacheValue;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.distribution.ch.DefaultConsistentHashFactory;
import org.infinispan.distribution.ch.ReplicatedConsistentHash;
import org.infinispan.distribution.ch.ReplicatedConsistentHashFactory;
import org.infinispan.distribution.ch.SyncConsistentHashFactory;
import org.infinispan.distribution.ch.TopologyAwareConsistentHashFactory;
import org.infinispan.distribution.ch.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.marshall.exts.ArrayListExternalizer;
import org.infinispan.marshall.exts.CacheRpcCommandExternalizer;
import org.infinispan.marshall.exts.LinkedListExternalizer;
import org.infinispan.marshall.exts.MapExternalizer;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.marshall.exts.SetExternalizer;
import org.infinispan.marshall.exts.SingletonListExternalizer;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.TransactionInfo;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTopologyAwareAddress;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.InDoubtTxInfoImpl;
import org.infinispan.transaction.xa.recovery.RecoveryAwareDldGlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareGlobalTransaction;
import org.infinispan.transaction.xa.recovery.SerializableXid;
import org.infinispan.util.ByteArrayKey;
import org.infinispan.util.Immutables;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import static org.infinispan.factories.KnownComponentNames.GLOBAL_MARSHALLER;

/**
 * The externalizer table maintains information necessary to be able to map a particular type with the corresponding
 * {@link org.infinispan.marshall.AdvancedExternalizer} implementation that it marshall, and it also keeps information of which {@link org.infinispan.marshall.AdvancedExternalizer}
 * should be used to read data from a buffer given a particular {@link org.infinispan.marshall.AdvancedExternalizer} identifier.
 *
 * These tables govern how either internal Infinispan classes, or user defined classes, are marshalled to a given
 * output, or how these are unmarshalled from a given input.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Scope(Scopes.GLOBAL)
public class ExternalizerTable implements ObjectTable {
   private static final Log log = LogFactory.getLog(ExternalizerTable.class);

   /**
    * Contains mapping of classes to their corresponding externalizer classes via ExternalizerAdapter instances.
    */
   private final Map<Class<?>, ExternalizerAdapter> writers = new WeakHashMap<Class<?>, ExternalizerAdapter>();

   /**
    * Contains mapping of ids to their corresponding AdvancedExternalizer classes via ExternalizerAdapter instances.
    * This maps contains mappings for both internal and foreign or user defined externalizers.
    *
    * Internal ids are only allowed to be unsigned bytes (0 to 254). 255 is an special id that signals the
    * arrival of a foreign externalizer id. Foreign externalizers are only allowed to use positive ids that between 0
    * and Integer.MAX_INT. To avoid clashes between foreign and internal ids, foreign ids are transformed into negative
    * values to be stored in this map. This way, we avoid the need of a second map to hold user defined externalizers.
    */
   private final Map<Integer, ExternalizerAdapter> readers = new HashMap<Integer, ExternalizerAdapter>();

   private volatile boolean started;

   private RemoteCommandsFactory cmdFactory;
   private GlobalComponentRegistry gcr;
   private StreamingMarshaller globalMarshaller;

   @Inject
   public void inject(RemoteCommandsFactory cmdFactory, GlobalComponentRegistry gcr,
         @ComponentName(GLOBAL_MARSHALLER) StreamingMarshaller globalMarshaller) {
      this.cmdFactory = cmdFactory;
      this.gcr = gcr;
      this.globalMarshaller = globalMarshaller;
   }

   @Start(priority = 7) // Should start before global marshaller
   public void start() {
      loadInternalMarshallables();
      loadForeignMarshallables(gcr.getGlobalConfiguration());
      started = true;
      if (log.isTraceEnabled()) {
         log.tracef("Constant object table was started and contains these externalizer readers: %s", readers);
         log.tracef("The externalizer writers collection contains: %s", writers);
      }
   }

   @Stop(priority = 13) // Stop after global marshaller
   public void stop() {
      writers.clear();
      readers.clear();
      started = false;
      log.trace("Externalizer reader and writer maps have been cleared and constant object table was stopped");
   }

   @Override
   public Writer getObjectWriter(Object o) throws IOException {
      Class<?> clazz = o.getClass();
      Writer writer = writers.get(clazz);
      if (writer == null) {
         if (Thread.currentThread().isInterrupted())
            throw new IOException(new InterruptedException(String.format(
                  "Cache manager is shutting down, so type write externalizer for type=%s cannot be resolved. Interruption being pushed up.",
                  clazz.getName())));
      }
      return writer;
   }

   @Override
   public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
      int readerIndex = input.readUnsignedByte();
      if (readerIndex == Ids.MAX_ID) // User defined externalizer
         readerIndex = generateForeignReaderIndex(UnsignedNumeric.readUnsignedInt(input));

      ExternalizerAdapter adapter = readers.get(readerIndex);
      if (adapter == null) {
         if (!started) {
            log.tracef("Either the marshaller has stopped or hasn't started. Read externalizers are not properly populated: %s", readers);

            if (Thread.currentThread().isInterrupted()) {
               throw new IOException(String.format(
                     "Cache manager is shutting down, so type (id=%d) cannot be resolved. Interruption being pushed up.",
                     readerIndex), new InterruptedException());
            } else {
               throw new CacheException(String.format(
                     "Cache manager is %s and type (id=%d) cannot be resolved (thread not interrupted)",
                     gcr.getStatus(), readerIndex));
            }
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("Unknown type. Input stream has %s to read", input.available());
               log.tracef("Check contents of read externalizers: %s", readers);
            }

            throw new CacheException(String.format(
                  "Type of data read is unknown. Id=%d is not amongst known reader indexes.",
                  readerIndex));
         }
      }

      return adapter.readObject(input);
   }

   boolean isMarshallableCandidate(Object o) {
      return writers.containsKey(o.getClass());
   }

   int getExternalizerId(Object o) {
      return writers.get(o.getClass()).getExternalizerId();
   }

   private void loadInternalMarshallables() {
      addInternalExternalizer(new ArrayListExternalizer());
      addInternalExternalizer(new LinkedListExternalizer());
      addInternalExternalizer(new MapExternalizer());
      addInternalExternalizer(new SetExternalizer());
      addInternalExternalizer(new SingletonListExternalizer());

      addInternalExternalizer(new GlobalTransaction.Externalizer());
      addInternalExternalizer(new RecoveryAwareGlobalTransaction.Externalizer());
      addInternalExternalizer(new DldGlobalTransaction.Externalizer());
      addInternalExternalizer(new RecoveryAwareDldGlobalTransaction.Externalizer());
      addInternalExternalizer(new JGroupsAddress.Externalizer());
      addInternalExternalizer(new Immutables.ImmutableMapWrapperExternalizer());
      addInternalExternalizer(new MarshalledValue.Externalizer(globalMarshaller));

      addInternalExternalizer(new SuccessfulResponse.Externalizer());
      addInternalExternalizer(new ExceptionResponse.Externalizer());
      addInternalExternalizer(new UnsuccessfulResponse.Externalizer());
      addInternalExternalizer(new UnsureResponse.Externalizer());

      ReplicableCommandExternalizer cmExt =
            new ReplicableCommandExternalizer(cmdFactory, gcr);
      addInternalExternalizer(cmExt);
      addInternalExternalizer(new CacheRpcCommandExternalizer(gcr, cmExt));

      addInternalExternalizer(new ImmortalCacheEntry.Externalizer());
      addInternalExternalizer(new MortalCacheEntry.Externalizer());
      addInternalExternalizer(new TransientCacheEntry.Externalizer());
      addInternalExternalizer(new TransientMortalCacheEntry.Externalizer());
      addInternalExternalizer(new ImmortalCacheValue.Externalizer());
      addInternalExternalizer(new MortalCacheValue.Externalizer());
      addInternalExternalizer(new TransientCacheValue.Externalizer());
      addInternalExternalizer(new TransientMortalCacheValue.Externalizer());

      addInternalExternalizer(new SimpleClusteredVersion.Externalizer());
      addInternalExternalizer(new VersionedImmortalCacheEntry.Externalizer());
      addInternalExternalizer(new VersionedMortalCacheEntry.Externalizer());
      addInternalExternalizer(new VersionedTransientCacheEntry.Externalizer());
      addInternalExternalizer(new VersionedTransientMortalCacheEntry.Externalizer());
      addInternalExternalizer(new VersionedImmortalCacheValue.Externalizer());
      addInternalExternalizer(new VersionedMortalCacheValue.Externalizer());
      addInternalExternalizer(new VersionedTransientCacheValue.Externalizer());
      addInternalExternalizer(new VersionedTransientMortalCacheValue.Externalizer());

      addInternalExternalizer(new ApplyDeltaCommand.DeltaCompositeKeyExternalizer());
      addInternalExternalizer(new AtomicHashMap.Externalizer());
      addInternalExternalizer(new Bucket.Externalizer());
      addInternalExternalizer(new AtomicHashMapDelta.Externalizer());
      addInternalExternalizer(new PutOperation.Externalizer());
      addInternalExternalizer(new RemoveOperation.Externalizer());
      addInternalExternalizer(new ClearOperation.Externalizer());
      addInternalExternalizer(new JGroupsTopologyAwareAddress.Externalizer());
      addInternalExternalizer(new ByteArrayKey.Externalizer());

      addInternalExternalizer(new SerializableXid.XidExternalizer());
      addInternalExternalizer(new InDoubtTxInfoImpl.Externalizer());

      addInternalExternalizer(new MurmurHash2.Externalizer());
      addInternalExternalizer(new MurmurHash2Compat.Externalizer());
      addInternalExternalizer(new MurmurHash3.Externalizer());

      addInternalExternalizer(new DefaultConsistentHash.Externalizer());
      addInternalExternalizer(new ReplicatedConsistentHash.Externalizer());
      addInternalExternalizer(new DefaultConsistentHashFactory.Externalizer());
      addInternalExternalizer(new ReplicatedConsistentHashFactory.Externalizer());
      addInternalExternalizer(new SyncConsistentHashFactory.Externalizer());
      addInternalExternalizer(new TopologyAwareConsistentHashFactory.Externalizer());
      addInternalExternalizer(new TopologyAwareSyncConsistentHashFactory.Externalizer());
      addInternalExternalizer(new CacheTopology.Externalizer());
      addInternalExternalizer(new CacheJoinInfo.Externalizer());
      addInternalExternalizer(new TransactionInfo.Externalizer());
      addInternalExternalizer(new StateChunk.Externalizer());

      addInternalExternalizer(new Flag.Externalizer());

      addInternalExternalizer(new InfinispanCollections.EmptySet.EmptySetExternalizer());
      addInternalExternalizer(new InfinispanCollections.EmptyMap.EmptyMapExternalizer());
      addInternalExternalizer(new InfinispanCollections.EmptyList.EmptyListExternalizer());
   }

   void addInternalExternalizer(AdvancedExternalizer<?> ext) {
      int id = checkInternalIdLimit(ext.getId(), ext);
      updateExtReadersWritersWithTypes(new ExternalizerAdapter(id, ext));
   }

   private void updateExtReadersWritersWithTypes(ExternalizerAdapter adapter) {
      updateExtReadersWritersWithTypes(adapter, adapter.id);
   }

   private void updateExtReadersWritersWithTypes(ExternalizerAdapter adapter, int readerIndex) {
      Set<Class<?>> typeClasses = adapter.externalizer.getTypeClasses();
      if (typeClasses.size() > 0) {
         for (Class<?> typeClass : typeClasses)
            updateExtReadersWriters(adapter, typeClass, readerIndex);
      } else {
         throw new ConfigurationException(String.format(
               "AdvancedExternalizer's getTypeClasses for externalizer %s must return a non-empty set",
               adapter.externalizer.getClass().getName()));
      }
   }

   private void loadForeignMarshallables(GlobalConfiguration globalCfg) {
      log.trace("Loading user defined externalizers");
      for (Map.Entry<Integer, AdvancedExternalizer<?>> config : globalCfg.serialization().advancedExternalizers().entrySet()) {
         AdvancedExternalizer<?> ext = config.getValue();

         // If no XML or programmatic config, id in annotation is used
         // as long as it's not default one (meaning, user did not set it).
         // If XML or programmatic config in use ignore @Marshalls annotation and use value in config.
         Integer id = ext.getId();
         if (config.getKey() == null && id == null)
            throw new ConfigurationException(String.format(
                  "No advanced externalizer identifier set for externalizer %s",
                  ext.getClass().getName()));
         else if (config.getKey() != null)
            id = config.getKey();

         id = checkForeignIdLimit(id, ext);
         updateExtReadersWritersWithTypes(new ForeignExternalizerAdapter(id, ext), generateForeignReaderIndex(id));
      }
   }

   private void updateExtReadersWriters(ExternalizerAdapter adapter, Class<?> typeClass, int readerIndex) {
      writers.put(typeClass, adapter);
      ExternalizerAdapter prevReader = readers.put(readerIndex, adapter);
      // Several externalizers might share same id (i.e. HashMap and TreeMap use MapExternalizer)
      // but a duplicate is only considered when that particular index has already been entered
      // in the readers map and the externalizers are different (they're from different classes)
      if (prevReader != null && !prevReader.equals(adapter))
         throw new ConfigurationException(String.format(
               "Duplicate id found! AdvancedExternalizer id=%d for %s is shared by another externalizer (%s). Reader index is %d",
               adapter.id, typeClass, prevReader.externalizer.getClass().getName(), readerIndex));

      if (log.isTraceEnabled())
         log.tracef("Loaded externalizer %s for %s with id %s and reader index %s",
                   adapter.externalizer.getClass().getName(), typeClass, adapter.id, readerIndex);

   }

   private int checkInternalIdLimit(int id, AdvancedExternalizer<?> ext) {
      if (id >= Ids.MAX_ID)
         throw new ConfigurationException(String.format(
               "Internal %s externalizer is using an id(%d) that exceeded the limit. It needs to be smaller than %d",
               ext, id, Ids.MAX_ID));
      return id;
   }

   private int checkForeignIdLimit(int id, AdvancedExternalizer<?> ext) {
      if (id < 0)
         throw new ConfigurationException(String.format(
               "Foreign %s externalizer is using a negative id(%d). Only positive id values are allowed.",
               ext, id));
      return id;
   }

   private int generateForeignReaderIndex(int foreignId) {
      return 0x80000000 | foreignId;
   }

   static class ExternalizerAdapter implements Writer {
      final int id;
      final AdvancedExternalizer<Object> externalizer;

      ExternalizerAdapter(int id, AdvancedExternalizer<?> externalizer) {
         this.id = id;
         this.externalizer = (AdvancedExternalizer<Object>) externalizer;
      }

      public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
         return externalizer.readObject(input);
      }

      @Override
      public void writeObject(Marshaller output, Object object) throws IOException {
         output.write(id);
         externalizer.writeObject(output, object);
      }

      int getExternalizerId() {
         return id;
      }

      @Override
      public String toString() {
         // Each adapter is represented by the externalizer it delegates to, so just return the class name
         return externalizer.getClass().getName();
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         ExternalizerAdapter that = (ExternalizerAdapter) o;
         if (id != that.id) return false;
         if (externalizer != null ? !externalizer.getClass().equals(that.externalizer.getClass()) : that.externalizer != null) return false;
         return true;
      }

      @Override
      public int hashCode() {
         int result = id;
         result = 31 * result + (externalizer.getClass() != null ? externalizer.getClass().hashCode() : 0);
         return result;
      }
   }

   static class ForeignExternalizerAdapter extends ExternalizerAdapter {
      final int foreignId;

      ForeignExternalizerAdapter(int foreignId, AdvancedExternalizer<?> externalizer) {
         super(Ids.MAX_ID, externalizer);
         this.foreignId = foreignId;
      }

      @Override
      int getExternalizerId() {
         return foreignId;
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
