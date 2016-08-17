package org.infinispan.marshall.core.internal;

import org.infinispan.atomic.DeltaCompositeKey;
import org.infinispan.atomic.impl.AtomicHashMap;
import org.infinispan.atomic.impl.AtomicHashMapDelta;
import org.infinispan.atomic.impl.ClearOperation;
import org.infinispan.atomic.impl.PutOperation;
import org.infinispan.atomic.impl.RemoveOperation;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.MarshallableFunctionExternalizers;
import org.infinispan.commons.marshall.SerializeFunctionWith;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.exts.EquivalenceExternalizer;
import org.infinispan.commons.util.Immutables;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataMortalCacheValue;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHash;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.distribution.ch.impl.SyncReplicatedConsistentHashFactory;
import org.infinispan.distribution.ch.impl.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.MetaParams;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.marshall.core.Ids;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.marshall.exts.ArrayExternalizer;
import org.infinispan.marshall.exts.CacheEventTypeExternalizer;
import org.infinispan.marshall.exts.CacheRpcCommandExternalizer;
import org.infinispan.marshall.exts.DoubleSummaryStatisticsExternalizer;
import org.infinispan.marshall.exts.EnumSetExternalizer;
import org.infinispan.marshall.exts.IntSummaryStatisticsExternalizer;
import org.infinispan.marshall.exts.ListExternalizer;
import org.infinispan.marshall.exts.LongSummaryStatisticsExternalizer;
import org.infinispan.marshall.exts.MapExternalizer;
import org.infinispan.marshall.exts.MetaParamExternalizers;
import org.infinispan.marshall.exts.OptionalExternalizer;
import org.infinispan.marshall.exts.QueueExternalizers;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.marshall.exts.SetExternalizer;
import org.infinispan.marshall.exts.SingletonListExternalizer;
import org.infinispan.marshall.exts.UuidExternalizer;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.notifications.cachelistener.cluster.ClusterEvent;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventCallable;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerRemoveCallable;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerReplicateCallable;
import org.infinispan.notifications.cachelistener.cluster.MultiClusterEventCallable;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterAsConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterAsKeyValueFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterAsKeyValueFilterConverter;
import org.infinispan.notifications.cachelistener.filter.KeyFilterAsCacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.KeyValueFilterAsCacheEventFilter;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.MIMECacheEntry;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTopologyAwareAddress;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.TransactionInfo;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.stream.impl.intops.IntermediateOperationExternalizer;
import org.infinispan.stream.impl.termop.TerminalOperationExternalizer;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.ManagerStatusResponse;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.InDoubtTxInfoImpl;
import org.infinispan.transaction.xa.recovery.RecoveryAwareDldGlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareGlobalTransaction;
import org.infinispan.transaction.xa.recovery.SerializableXid;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteState;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

final class InternalExternalizerTable {

   private static final Log log = LogFactory.getLog(InternalMarshaller.class);
   private static final boolean trace = log.isTraceEnabled();

   /**
    * Externalizer for primitives
    */
   private final PrimitiveExternalizer primitives;

   /**
    * Contains mapping of classes to their corresponding externalizer classes via ExternalizerAdapter instances.
    */
   private final Map<Class<?>, AdvancedExternalizer<?>> writers = new WeakHashMap<>();

   /**
    * Contains mapping of ids to their corresponding AdvancedExternalizer classes via ExternalizerAdapter instances.
    * This maps contains mappings for both internal and foreign or user defined externalizers.
    *
    * Internal ids are only allowed to be unsigned bytes (0 to 254). 255 is an special id that signals the
    * arrival of a foreign externalizer id. Foreign externalizers are only allowed to use positive ids that between 0
    * and Integer.MAX_INT. To avoid clashes between foreign and internal ids, foreign ids are transformed into negative
    * values to be stored in this map. This way, we avoid the need of a second map to hold user defined externalizers.
    */
   private final Map<Integer, AdvancedExternalizer<?>> readers = new HashMap<>();

   private final GlobalComponentRegistry gcr;
   private final RemoteCommandsFactory cmdFactory;

   InternalExternalizerTable(Encoding enc, GlobalComponentRegistry gcr, RemoteCommandsFactory cmdFactory) {
      this.primitives = new PrimitiveExternalizer(enc);
      this.gcr = gcr;
      this.cmdFactory = cmdFactory;
   }

   void start() {
      loadInternalMarshallables();
      loadForeignMarshallables(gcr.getGlobalConfiguration());
      if (trace) {
         log.tracef("Constant object table was started and contains these externalizer readers: %s", readers);
         log.tracef("The externalizer writers collection contains: %s", writers);
      }
   }

   void stop() {
      writers.clear();
      readers.clear();
      log.trace("Externalizer reader and writer maps have been cleared and constant object table was stopped");
   }

   <T> Externalizer<T> findWriteExternalizer(Object obj, ObjectOutput out) throws IOException {
      Class<?> clazz = obj == null ? null : obj.getClass();
      Externalizer<T> ext;
      if (clazz == null || primitives.getTypeClasses().contains(clazz)) {
         out.writeByte(InternalIds.PRIMITIVE);
         ext = (Externalizer<T>) primitives;
      } else {
         ext = (Externalizer<T>) writers.get(clazz);
         if (ext != null) {
            if (ext instanceof ForeignExternalizerAdapter) {
               out.writeByte(InternalIds.PRE_CONFIGURED);
               UnsignedNumeric.writeUnsignedInt(out, ((AdvancedExternalizer<T>) ext).getId());
            } else {
               out.writeByte(InternalIds.NON_PRIMITIVE);
               out.writeByte(((AdvancedExternalizer<T>) ext).getId());
            }
         } else if ((ext = findAnnotatedExternalizer(clazz)) != null) {
            out.writeByte(InternalIds.ANNOTATED);
            out.writeObject(ext.getClass());
         } else {
            out.writeByte(InternalIds.EXTERNAL);
         }
      }
      return ext;
   }

   <T> Externalizer<T> findAnnotatedExternalizer(Class<?> clazz) {
      try {
         SerializeWith serialAnn = clazz.getAnnotation(SerializeWith.class);
         if (serialAnn != null) {
            return (Externalizer<T>) serialAnn.value().newInstance();
         } else {
            SerializeFunctionWith funcSerialAnn = clazz.getAnnotation(SerializeFunctionWith.class);
            if (funcSerialAnn != null)
               return (Externalizer<T>) funcSerialAnn.value().newInstance();
         }

         return null;
      } catch (Exception e) {
         throw new IllegalArgumentException(String.format(
               "Cannot instantiate externalizer for %s", clazz), e);
      }
   }


   <T> Externalizer<T> findReadExternalizer(ObjectInput in) {
      try {
         // Check if primitive or non-primitive
         int type = in.readUnsignedByte();
         switch (type) {
            case InternalIds.PRIMITIVE:
               return (Externalizer<T>) primitives;
            case InternalIds.NON_PRIMITIVE:
               int subType = in.readUnsignedByte();
               Externalizer<T> ext = (Externalizer<T>) readers.get(subType);
               // TODO: Add null checks and see if not started...etc
               return ext;
            case InternalIds.ANNOTATED:
               Class<? extends Externalizer<T>> clazz =
                     (Class<? extends Externalizer<T>>) in.readObject();
               return clazz.newInstance();
            case InternalIds.PRE_CONFIGURED:
               int foreignId = UnsignedNumeric.readUnsignedInt(in);
               int foreignSubType = generateForeignReaderIndex(foreignId);
               // TODO: Add null checks and see if not started...etc
               return (Externalizer<T>) readers.get(foreignSubType);
            case InternalIds.EXTERNAL:
               return null;
            default:
               throw new CacheException("Unknown externalizer type: " + type);
         }

//      int foreignId = -1;
//      if (readerIndex == Ids.MAX_ID) {
//         // User defined externalizer
//         foreignId = UnsignedNumeric.readUnsignedInt(input);
//         readerIndex = generateForeignReaderIndex(foreignId);
//      }

//         AdvancedExternalizer<T> ext = (AdvancedExternalizer<T>) readers.get(readerIndex);
//         if (ext == null) {
//            if (!started) {
//               log.tracef("Either the marshaller has stopped or hasn't started. Read externalizers are not properly populated: %s", readers);
//
////            if (Thread.currentThread().isInterrupted()) {
////               throw log.pushReadInterruptionDueToCacheManagerShutdown(readerIndex, new InterruptedException());
////            } else {
////               throw log.cannotResolveExternalizerReader(gcr.getStatus(), readerIndex);
////            }
//            }
////            else {
//////               if (trace) {
//////                  // TODO: Implement available() in BytesObjectInput ?
//////                  log.tracef("Unknown type. Input stream has %s to read", input.available());
//////                  log.tracef("Check contents of read externalizers: %s", readers);
//////               }
////
//////            if (foreignId > 0)
//////               throw log.missingForeignExternalizer(foreignId);
////
////               throw log.unknownExternalizerReaderIndex(readerIndex);
////            }
//         }
//
//         return ext;
      }
      catch (Exception e) {
         // TODO: Update Log.java eventually (not doing yet to avoid need to rebase)
         throw new CacheException("Error reading from input to find externalizer", e);
      }
   }

   boolean isMarshallable(Object o) {
      Class<?> clazz = o.getClass();
      return primitives.getTypeClasses().contains(clazz)
            || writers.containsKey(clazz);
   }

   private void loadInternalMarshallables() {
      StreamingMarshaller marshaller = gcr.getComponent(StreamingMarshaller.class);

      ReplicableCommandExternalizer ext = new ReplicableCommandExternalizer(cmdFactory, gcr);
      addInternalExternalizer(ext);

      addInternalExternalizer(new AcceptAllKeyValueFilter.Externalizer());
      addInternalExternalizer(new ArrayExternalizer());
      addInternalExternalizer(new AtomicHashMap.Externalizer());
      addInternalExternalizer(new AtomicHashMapDelta.Externalizer());
      addInternalExternalizer(new AvailabilityMode.Externalizer());
      addInternalExternalizer(new ByteBufferImpl.Externalizer());
      addInternalExternalizer(new CacheEventConverterAsConverter.Externalizer());
      addInternalExternalizer(new CacheEventFilterAsKeyValueFilter.Externalizer());
      addInternalExternalizer(new CacheEventFilterConverterAsKeyValueFilterConverter.Externalizer());
      addInternalExternalizer(new CacheEventTypeExternalizer());
      addInternalExternalizer(new CacheFilters.CacheFiltersExternalizer());
      addInternalExternalizer(new CacheJoinInfo.Externalizer());
      addInternalExternalizer(new CacheNotFoundResponse.Externalizer());
      addInternalExternalizer(new CacheRpcCommandExternalizer(gcr, ext));
      addInternalExternalizer(new CacheStatusResponse.Externalizer());
      addInternalExternalizer(new CacheTopology.Externalizer());
      addInternalExternalizer(new ClearOperation.Externalizer());
      addInternalExternalizer(new ClusterEvent.Externalizer());
      addInternalExternalizer(new ClusterEventCallable.Externalizer());
      addInternalExternalizer(new ClusterListenerRemoveCallable.Externalizer());
      addInternalExternalizer(new ClusterListenerReplicateCallable.Externalizer());
      addInternalExternalizer(new CollectionKeyFilter.Externalizer());
      addInternalExternalizer(new DefaultConsistentHash.Externalizer());
      addInternalExternalizer(new DeltaCompositeKey.DeltaCompositeKeyExternalizer());
      addInternalExternalizer(new DldGlobalTransaction.Externalizer());
      addInternalExternalizer(new DoubleSummaryStatisticsExternalizer());
      addInternalExternalizer(new EmbeddedMetadata.Externalizer());
      addInternalExternalizer(new EntryViews.ReadWriteSnapshotViewExternalizer());
      addInternalExternalizer(new EnumSetExternalizer());
      addInternalExternalizer(new EquivalenceExternalizer());
      addInternalExternalizer(new ExceptionResponse.Externalizer());
      addInternalExternalizer(new Flag.Externalizer());
      addInternalExternalizer(new GlobalTransaction.Externalizer());
      addInternalExternalizer(new KeyFilterAsCacheEventFilter.Externalizer());
      addInternalExternalizer(new KeyFilterAsKeyValueFilter.Externalizer());
      addInternalExternalizer(new KeyValueFilterAsCacheEventFilter.Externalizer());
      addInternalExternalizer(new ImmortalCacheEntry.Externalizer());
      addInternalExternalizer(new ImmortalCacheValue.Externalizer());
      addInternalExternalizer(new Immutables.ImmutableMapWrapperExternalizer());
      addInternalExternalizer(new Immutables.ImmutableSetWrapperExternalizer());
      addInternalExternalizer(new InDoubtTxInfoImpl.Externalizer());
      addInternalExternalizer(new IntermediateOperationExternalizer());
      addInternalExternalizer(new InternalMetadataImpl.Externalizer());
      addInternalExternalizer(new IntSummaryStatisticsExternalizer());
      addInternalExternalizer(new JGroupsAddress.Externalizer());
      addInternalExternalizer(new JGroupsTopologyAwareAddress.Externalizer());
      addInternalExternalizer(new ListExternalizer());
      addInternalExternalizer(new LongSummaryStatisticsExternalizer());
      addInternalExternalizer(new KeyValuePair.Externalizer());
      addInternalExternalizer(new ManagerStatusResponse.Externalizer());
      addInternalExternalizer(new MapExternalizer());
      addInternalExternalizer(new MarshallableFunctionExternalizers.ConstantLambdaExternalizer());
      addInternalExternalizer(new MarshallableFunctionExternalizers.LambdaWithMetasExternalizer());
      addInternalExternalizer(new MarshallableFunctionExternalizers.SetValueIfEqualsReturnBooleanExternalizer());
      addInternalExternalizer(new MarshalledEntryImpl.Externalizer(marshaller));
      addInternalExternalizer(new MarshalledValue.Externalizer(marshaller));
      addInternalExternalizer(new MetadataImmortalCacheEntry.Externalizer());
      addInternalExternalizer(new MetadataImmortalCacheValue.Externalizer());
      addInternalExternalizer(new MetadataMortalCacheValue.Externalizer());
      addInternalExternalizer(new MetaParamExternalizers.LifespanExternalizer());
      addInternalExternalizer(new MetaParamExternalizers.EntryVersionParamExternalizer());
      addInternalExternalizer(new MetaParamExternalizers.NumericEntryVersionExternalizer());
      addInternalExternalizer(new MetaParams.Externalizer());
      addInternalExternalizer(new MetaParamsInternalMetadata.Externalizer());
      addInternalExternalizer(new MIMECacheEntry.Externalizer()); // new
      addInternalExternalizer(new MortalCacheEntry.Externalizer());
      addInternalExternalizer(new MortalCacheValue.Externalizer());
      addInternalExternalizer(new MultiClusterEventCallable.Externalizer());
      addInternalExternalizer(new MurmurHash3.Externalizer());
      addInternalExternalizer(new OptionalExternalizer());
      addInternalExternalizer(new PersistentUUID.Externalizer());
      addInternalExternalizer(new PutOperation.Externalizer());
      addInternalExternalizer(new QueueExternalizers());
      addInternalExternalizer(new RecoveryAwareDldGlobalTransaction.Externalizer());
      addInternalExternalizer(new RecoveryAwareGlobalTransaction.Externalizer());
      addInternalExternalizer(new RemoveOperation.Externalizer());
      addInternalExternalizer(new ReplicatedConsistentHash.Externalizer());
      addInternalExternalizer(new SerializableXid.XidExternalizer());
      addInternalExternalizer(new SetExternalizer());
      addInternalExternalizer(new SimpleClusteredVersion.Externalizer());
      addInternalExternalizer(new SingletonListExternalizer());
      addInternalExternalizer(new StateChunk.Externalizer());
      addInternalExternalizer(new StreamMarshalling.StreamMarshallingExternalizer());
      addInternalExternalizer(new SuccessfulResponse.Externalizer());
      addInternalExternalizer(new SyncConsistentHashFactory.Externalizer());
      addInternalExternalizer(new SyncReplicatedConsistentHashFactory.Externalizer());
      addInternalExternalizer(new TerminalOperationExternalizer());
      addInternalExternalizer(new TopologyAwareSyncConsistentHashFactory.Externalizer());
      addInternalExternalizer(new TransactionInfo.Externalizer());
      addInternalExternalizer(new TransientCacheEntry.Externalizer());
      addInternalExternalizer(new TransientCacheValue.Externalizer());
      addInternalExternalizer(new TransientMortalCacheEntry.Externalizer());
      addInternalExternalizer(new TransientMortalCacheValue.Externalizer());
      addInternalExternalizer(new UnsuccessfulResponse.Externalizer());
      addInternalExternalizer(new UnsureResponse.Externalizer());
      addInternalExternalizer(new UuidExternalizer());
      addInternalExternalizer(new XSiteState.XSiteStateExternalizer());
   }

   private void addInternalExternalizer(AdvancedExternalizer<?> ext) {
      int id = checkInternalIdLimit(ext.getId(), ext);
      updateExtReadersWritersWithTypes(ext, id);
   }

   private int checkInternalIdLimit(int id, AdvancedExternalizer<?> ext) {
      if (id >= Ids.MAX_ID)
         throw log.internalExternalizerIdLimitExceeded(ext, id, Ids.MAX_ID);

      return id;
   }

   private void updateExtReadersWritersWithTypes(AdvancedExternalizer ext, int readerIndex) {
      Set<Class<?>> typeClasses = ext.getTypeClasses();
      if (typeClasses.size() > 0) {
         for (Class<?> typeClass : typeClasses)
            updateExtReadersWriters(ext, typeClass, readerIndex);
      } else {
         throw log.advanceExternalizerTypeClassesUndefined(ext.getClass().getName());
      }
   }

   private void updateExtReadersWriters(AdvancedExternalizer<?> ext, Class<?> typeClass, int readerIndex) {
      writers.put(typeClass, ext);
      AdvancedExternalizer<?> prevReader = readers.put(readerIndex, ext);
      // Several externalizers might share same id (i.e. HashMap and TreeMap use MapExternalizer)
      // but a duplicate is only considered when that particular index has already been entered
      // in the readers map and the externalizers are different (they're from different classes)
      if (prevReader != null && !prevReader.equals(ext))
         throw log.duplicateExternalizerIdFound(
               ext.getId(), typeClass, prevReader.getClass().getName(), readerIndex);

      if (trace)
         log.tracef("Loaded externalizer %s for %s with id %s and reader index %s",
               ext.getClass().getName(), typeClass, ext.getId(), readerIndex);
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
            throw new CacheConfigurationException(String.format(
                  "No advanced externalizer identifier set for externalizer %s",
                  ext.getClass().getName()));
         else if (config.getKey() != null)
            id = config.getKey();

         id = checkForeignIdLimit(id, ext);
         updateExtReadersWritersWithTypes(new ForeignExternalizerAdapter(id, ext), generateForeignReaderIndex(id));
      }
   }

   private int checkForeignIdLimit(int id, AdvancedExternalizer<?> ext) {
      if (id < 0)
         throw log.foreignExternalizerUsingNegativeId(ext, id);

      return id;
   }

   private int generateForeignReaderIndex(int foreignId) {
      return 0x80000000 | foreignId;
   }

   static final class ForeignExternalizerAdapter extends AbstractExternalizer<Object> {

      final int foreignId;
      final AdvancedExternalizer<Object> ext;

      ForeignExternalizerAdapter(int foreignId, AdvancedExternalizer<?> ext) {
         this.foreignId = foreignId;
         this.ext = (AdvancedExternalizer<Object>) ext;
      }

      @Override
      public Set<Class<?>> getTypeClasses() {
         return ext.getTypeClasses();
      }

      @Override
      public Integer getId() {
         return foreignId;
      }

      @Override
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         ext.writeObject(output, object);
      }

      @Override
      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return ext.readObject(input);
      }

   }

}
