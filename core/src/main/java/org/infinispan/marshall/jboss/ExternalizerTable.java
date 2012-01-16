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
import org.infinispan.cacheviews.CacheView;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commons.hash.MurmurHash2;
import org.infinispan.commons.hash.MurmurHash2Compat;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.config.AdvancedExternalizerConfig;
import org.infinispan.config.ConfigurationException;
import org.infinispan.config.GlobalConfiguration;
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
import org.infinispan.distribution.RemoteTransactionLogDetails;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.distribution.ch.TopologyAwareConsistentHash;
import org.infinispan.distribution.ch.UnionConsistentHash;
import org.infinispan.factories.GlobalComponentRegistry;
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
import org.infinispan.marshall.exts.ArrayListExternalizer;
import org.infinispan.marshall.exts.CacheRpcCommandExternalizer;
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
import org.infinispan.remoting.transport.jgroups.JGroupsTopologyAwareAddress;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.InDoubtTxInfoImpl;
import org.infinispan.transaction.xa.recovery.RecoveryAwareDldGlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareGlobalTransaction;
import org.infinispan.transaction.xa.recovery.SerializableXid;
import org.infinispan.util.ByteArrayKey;
import org.infinispan.util.Immutables;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

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
   private final Set<AdvancedExternalizer> internalExternalizers = new HashSet<AdvancedExternalizer>();

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

   private void initInternalExternalizers() {
      internalExternalizers.add(new ArrayListExternalizer());
      internalExternalizers.add(new LinkedListExternalizer());
      internalExternalizers.add(new MapExternalizer());
      internalExternalizers.add(new SetExternalizer());
      internalExternalizers.add(new SingletonListExternalizer());

      internalExternalizers.add(new GlobalTransaction.Externalizer());
      internalExternalizers.add(new RecoveryAwareGlobalTransaction.Externalizer());
      internalExternalizers.add(new DldGlobalTransaction.Externalizer());
      internalExternalizers.add(new RecoveryAwareDldGlobalTransaction.Externalizer());
      internalExternalizers.add(new JGroupsAddress.Externalizer());
      internalExternalizers.add(new Immutables.ImmutableMapWrapperExternalizer());
      internalExternalizers.add(new MarshalledValue.Externalizer());

      internalExternalizers.add(new ExtendedResponse.Externalizer());
      internalExternalizers.add(new SuccessfulResponse.Externalizer());
      internalExternalizers.add(new ExceptionResponse.Externalizer());
      internalExternalizers.add(new RequestIgnoredResponse.Externalizer());
      internalExternalizers.add(new UnsuccessfulResponse.Externalizer());
      internalExternalizers.add(new UnsureResponse.Externalizer());

      internalExternalizers.add(new ReplicableCommandExternalizer());
      internalExternalizers.add(new CacheRpcCommandExternalizer());

      internalExternalizers.add(new ImmortalCacheEntry.Externalizer());
      internalExternalizers.add(new MortalCacheEntry.Externalizer());
      internalExternalizers.add(new TransientCacheEntry.Externalizer());
      internalExternalizers.add(new TransientMortalCacheEntry.Externalizer());
      internalExternalizers.add(new ImmortalCacheValue.Externalizer());
      internalExternalizers.add(new MortalCacheValue.Externalizer());
      internalExternalizers.add(new TransientCacheValue.Externalizer());
      internalExternalizers.add(new TransientMortalCacheValue.Externalizer());

      internalExternalizers.add(new VersionedImmortalCacheEntry.Externalizer());
      internalExternalizers.add(new VersionedMortalCacheEntry.Externalizer());
      internalExternalizers.add(new VersionedTransientCacheEntry.Externalizer());
      internalExternalizers.add(new VersionedTransientMortalCacheEntry.Externalizer());
      internalExternalizers.add(new VersionedImmortalCacheValue.Externalizer());
      internalExternalizers.add(new VersionedMortalCacheValue.Externalizer());
      internalExternalizers.add(new VersionedTransientCacheValue.Externalizer());
      internalExternalizers.add(new VersionedTransientMortalCacheValue.Externalizer());

      internalExternalizers.add(new AtomicHashMap.Externalizer());
      internalExternalizers.add(new Bucket.Externalizer());
      internalExternalizers.add(new AtomicHashMapDelta.Externalizer());
      internalExternalizers.add(new PutOperation.Externalizer());
      internalExternalizers.add(new RemoveOperation.Externalizer());
      internalExternalizers.add(new ClearOperation.Externalizer());
      internalExternalizers.add(new DefaultConsistentHash.Externalizer());
      internalExternalizers.add(new UnionConsistentHash.Externalizer());
      internalExternalizers.add(new JGroupsTopologyAwareAddress.Externalizer());
      internalExternalizers.add(new TopologyAwareConsistentHash.Externalizer());
      internalExternalizers.add(new ByteArrayKey.Externalizer());

      internalExternalizers.add(new RemoteTransactionLogDetails.Externalizer());
      internalExternalizers.add(new SerializableXid.XidExternalizer());
      internalExternalizers.add(new InDoubtTxInfoImpl.Externalizer());

      internalExternalizers.add(new MurmurHash2.Externalizer());
      internalExternalizers.add(new MurmurHash2Compat.Externalizer());
      internalExternalizers.add(new MurmurHash3.Externalizer());

      internalExternalizers.add(new CacheView.Externalizer());
   }

   void addInternalExternalizer(AdvancedExternalizer ext) {
      internalExternalizers.add(ext);
   }

   @Inject
   public void inject(RemoteCommandsFactory cmdFactory, GlobalComponentRegistry gcr) {
      this.cmdFactory = cmdFactory;
      this.gcr = gcr;
   }

   @Start(priority = 7) // Should start before global marshaller
   public void start() {
      initInternalExternalizers();
      loadInternalMarshallables(cmdFactory, gcr);
      loadForeignMarshallables(gcr.getGlobalConfiguration());
      started = true;
      if (log.isTraceEnabled()) {
         log.tracef("Constant object table was started and contains these externalizer readers: %s", readers);
         log.tracef("The externalizer writers collection contains: %s", writers);
      }
   }

   @Stop(priority = 13) // Stop after global marshaller
   public void stop() {
      internalExternalizers.clear();
      writers.clear();
      readers.clear();
      started = false;
      if (log.isTraceEnabled())
         log.trace("Externalizer reader and writer maps have been cleared and constant object table was stopped");
   }

   public Writer getObjectWriter(Object o) throws IOException {
      Class clazz = o.getClass();
      Writer writer = writers.get(clazz);
      if (writer == null) {
         if (Thread.currentThread().isInterrupted())
            throw new IOException(new InterruptedException(String.format(
                  "Cache manager is shutting down, so type write externalizer for type=%s cannot be resolved. Interruption being pushed up.",
                  clazz.getName())));
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
               log.tracef("Either the marshaller has stopped or hasn't started. Read externalizers are not propery populated: %s", readers);

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

   private void loadInternalMarshallables(RemoteCommandsFactory cmdFactory, GlobalComponentRegistry gcr) {
      for (AdvancedExternalizer ext : internalExternalizers) {
         if (ext instanceof ReplicableCommandExternalizer)
            ((ReplicableCommandExternalizer) ext).inject(cmdFactory, gcr);
         if (ext instanceof CacheRpcCommandExternalizer)
            ((CacheRpcCommandExternalizer) ext).inject(cmdFactory, gcr);
         if (ext instanceof MarshalledValue.Externalizer)
            ((MarshalledValue.Externalizer) ext).inject(gcr);

         int id = checkInternalIdLimit(ext.getId(), ext);
         updateExtReadersWritersWithTypes(new ExternalizerAdapter(id, ext));
      }
   }

   private void updateExtReadersWritersWithTypes(ExternalizerAdapter adapter) {
      updateExtReadersWritersWithTypes(adapter, adapter.id);
   }

   private void updateExtReadersWritersWithTypes(ExternalizerAdapter adapter, int readerIndex) {
      Set<Class> typeClasses = adapter.externalizer.getTypeClasses();
      if (typeClasses.size() > 0) {
         for (Class typeClass : typeClasses)
            updateExtReadersWriters(adapter, typeClass, readerIndex);
      } else {
         throw new ConfigurationException(String.format(
               "AdvancedExternalizer's getTypeClasses for externalizer %s must return a non-empty set",
               adapter.externalizer.getClass().getName()));
      }
   }

   private void loadForeignMarshallables(GlobalConfiguration globalCfg) {
      if (log.isTraceEnabled())
         log.trace("Loading user defined externalizers");
      List<AdvancedExternalizerConfig> configs = globalCfg.getExternalizers();
      for (AdvancedExternalizerConfig config : configs) {
         AdvancedExternalizer ext = config.getAdvancedExternalizer() != null ? config.getAdvancedExternalizer()
               : (AdvancedExternalizer) Util.getInstance(config.getExternalizerClass(), globalCfg.getClassLoader());

         // If no XML or programmatic config, id in annotation is used
         // as long as it's not default one (meaning, user did not set it).
         // If XML or programmatic config in use ignore @Marshalls annotation and use value in config.
         Integer id = ext.getId();
         if (config.getId() == null && id == null)
            throw new ConfigurationException(String.format(
                  "No advanced externalizer identifier set for externalizer %s",
                  ext.getClass().getName()));
         else if (config.getId() != null)
            id = config.getId();

         id = checkForeignIdLimit(id, ext);
         updateExtReadersWritersWithTypes(new ForeignExternalizerAdapter(id, ext), generateForeignReaderIndex(id));
      }
   }

   private void updateExtReadersWriters(ExternalizerAdapter adapter, Class typeClass, int readerIndex) {
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

   private int checkInternalIdLimit(int id, AdvancedExternalizer ext) {
      if (id >= Ids.MAX_ID)
         throw new ConfigurationException(String.format(
               "Internal %s externalizer is using an id(%d) that exceeed the limit. It needs to be smaller than %d",
               ext, id, Ids.MAX_ID));
      return id;
   }

   private int checkForeignIdLimit(int id, AdvancedExternalizer ext) {
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
      final AdvancedExternalizer externalizer;

      ExternalizerAdapter(int id, AdvancedExternalizer externalizer) {
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

      ForeignExternalizerAdapter(int foreignId, AdvancedExternalizer externalizer) {
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
