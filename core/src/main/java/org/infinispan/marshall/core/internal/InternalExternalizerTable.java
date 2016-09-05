package org.infinispan.marshall.core.internal;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

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
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheEntry;
import org.infinispan.container.versioning.NumericVersion;
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
import org.infinispan.util.IntObjectHashMap;
import org.infinispan.util.IntObjectMap;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.jboss.marshalling.util.IdentityIntMap;

final class InternalExternalizerTable {

   private static final Log log = LogFactory.getLog(InternalMarshaller.class);
   private static final boolean trace = log.isTraceEnabled();

   private static final int NOT_FOUND = -1;

   /**
    * Encoding of the internal marshaller
    */
   private final Encoding enc;

   /**
    * Pairing between internally marshallable types and their numeric identifiers.
    * Externalizer identifiers are decided based on order of internal externalizer
    * definition below. Such limitation speeds up internal externalizer lookups
    * at read time using a simple array of externalizers.
    *
    * This collection is sized according to an approximation of number of
    * internal types.
    */
   final IdentityIntMap<Class<?>> internalExtIds = new IdentityIntMap<>(320);

   /**
    * Array of internal externalizers used for externalizer lookups on read time.
    * Since each externalizer's id is within a sequence of numbers decided on
    * startup, lookups can be done via an array making it very efficient.
    *
    * This collection is sized according to an approximation of number of
    * internal externalizers.
    */
   final AdvancedExternalizer[] internalExts = new AdvancedExternalizer[128];

   /**
    * Pairing between types marshalled by the externalizers predefined via
    * global configuration and their externalizer identifiers.
    */
   final IdentityIntMap<Class<?>> predefExtIds = new IdentityIntMap<>(32);

   /**
    * Pairing between identifiers for externalizers defined via global
    * configuration, and these externalizers.
    */
   final IntObjectMap<AdvancedExternalizer> predefExts = new IntObjectHashMap<>(16);

   private final GlobalComponentRegistry gcr;
   private final RemoteCommandsFactory cmdFactory;

   InternalExternalizerTable(Encoding enc, GlobalComponentRegistry gcr, RemoteCommandsFactory cmdFactory) {
      this.enc = enc;
      this.gcr = gcr;
      this.cmdFactory = cmdFactory;
   }

   void start() {
      loadInternalMarshallables();
      if (trace) {
         log.tracef("Internal externalizer ids: %s", internalExtIds);
         log.tracef("Internal externalizers: %s", toStringWithIndex(internalExts));
      }

      loadForeignMarshallables(gcr.getGlobalConfiguration());
      if (trace) {
         log.tracef("Predefined externalizer ids: %s", predefExtIds);
         log.tracef("Predefined externalizers: %s", predefExts);
      }
   }

   static String toStringWithIndex(Object[] arr) {
      StringJoiner sj = new StringJoiner(", ", "[", "]");
      for (int i = 0; i < arr.length; i++)
         sj.add(i + ":" + arr[i]);

      return sj.toString();
   }

   void stop() {
      internalExtIds.clear();
      predefExtIds.clear();
      Arrays.fill(internalExts, null);
      predefExts.clear();
      log.trace("Internal externalizer table has stopped");
   }

   <T> Externalizer<T> findWriteExternalizer(Object obj, ObjectOutput out) throws IOException {
      Class<?> clazz = obj == null ? null : obj.getClass();
      Externalizer<T> ext;
      if (clazz == null) {
         out.writeByte(InternalIds.NULL);
         ext = (Externalizer<T>) internalExts[0];
      } else {
         int extId = internalExtIds.get(clazz, -1);
         if (extId != -1) {
            ext = internalExts[extId];
            out.writeByte(InternalIds.INTERNAL);
            out.writeByte(extId);
         } else {
            int foreignExtId = predefExtIds.get(clazz, -1);
            if (foreignExtId != -1) {
               ext = predefExts.get(foreignExtId);
               out.writeByte(InternalIds.PREDEFINED);
               UnsignedNumeric.writeUnsignedInt(out, foreignExtId);
            } else if ((ext = findAnnotatedExternalizer(clazz)) != null) {
               out.writeByte(InternalIds.ANNOTATED);
               out.writeObject(ext.getClass());
            } else {
               out.writeByte(InternalIds.EXTERNAL);
            }
         }
      }

      return ext;
   }

   private <T> Externalizer<T> findAnnotatedExternalizer(Class<?> clazz) {
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
            case InternalIds.NULL:
               return internalExts[0];
            case InternalIds.INTERNAL:
               int extId = in.readUnsignedByte();
               return internalExts[extId];
            case InternalIds.PREDEFINED:
               int predefinedId = UnsignedNumeric.readUnsignedInt(in);
               return predefExts.get(predefinedId);
            case InternalIds.ANNOTATED:
               Class<? extends Externalizer<T>> clazz =
                     (Class<? extends Externalizer<T>>) in.readObject();
               return clazz.newInstance();
            case InternalIds.EXTERNAL:
               return null;
            default:
               throw new CacheException("Unknown externalizer type: " + type);
         }
      }
      catch (Exception e) {
         // TODO: Update Log.java eventually (not doing yet to avoid need to rebase)
         throw new CacheException("Error reading from input to find externalizer", e);
      }
   }

   MarshallableType marshallable(Object o) {
      Class<?> clazz = o.getClass();
      int extId = internalExtIds.get(clazz, NOT_FOUND);
      if (extId == 0) {
         return MarshallableType.PRIMITIVE;
      } else if (extId != NOT_FOUND) {
         return MarshallableType.INTERNAL;
      } else if (hasExternalizer(clazz, predefExtIds)) {
         return MarshallableType.PREDEFINED;
      } else if (findAnnotatedExternalizer(clazz) != null) {
         return MarshallableType.ANNOTATED;
      } else {
         return MarshallableType.NOT_MARSHALLABLE;
      }
   }

   private boolean hasExternalizer(Class<?> clazz, IdentityIntMap<Class<?>> col) {
      return col.get(clazz, NOT_FOUND) != NOT_FOUND;
   }

   private void loadInternalMarshallables() {
      StreamingMarshaller marshaller = gcr.getComponent(StreamingMarshaller.class);

      int extId = 0;

      extId = addInternalExternalizer(new PrimitiveExternalizer(enc), extId);

      ReplicableCommandExternalizer ext = new ReplicableCommandExternalizer(cmdFactory, gcr);
      extId = addInternalExternalizer(ext, extId);

      extId = addInternalExternalizer(new AcceptAllKeyValueFilter.Externalizer(), extId);
      extId = addInternalExternalizer(new ArrayExternalizer(), extId);
      extId = addInternalExternalizer(new AtomicHashMap.Externalizer(), extId);
      extId = addInternalExternalizer(new AtomicHashMapDelta.Externalizer(), extId);
      extId = addInternalExternalizer(new AvailabilityMode.Externalizer(), extId);
      extId = addInternalExternalizer(new ByteBufferImpl.Externalizer(), extId);
      extId = addInternalExternalizer(new CacheEventConverterAsConverter.Externalizer(), extId);
      extId = addInternalExternalizer(new CacheEventFilterAsKeyValueFilter.Externalizer(), extId);
      extId = addInternalExternalizer(new CacheEventFilterConverterAsKeyValueFilterConverter.Externalizer(), extId);
      extId = addInternalExternalizer(new CacheEventTypeExternalizer(), extId);
      extId = addInternalExternalizer(new CacheFilters.CacheFiltersExternalizer(), extId);
      extId = addInternalExternalizer(new CacheJoinInfo.Externalizer(), extId);
      extId = addInternalExternalizer(new CacheNotFoundResponse.Externalizer(), extId);
      extId = addInternalExternalizer(new CacheRpcCommandExternalizer(gcr, ext), extId);
      extId = addInternalExternalizer(new CacheStatusResponse.Externalizer(), extId);
      extId = addInternalExternalizer(new CacheTopology.Externalizer(), extId);
      extId = addInternalExternalizer(new ClearOperation.Externalizer(), extId);
      extId = addInternalExternalizer(new ClusterEvent.Externalizer(), extId);
      extId = addInternalExternalizer(new ClusterEventCallable.Externalizer(), extId);
      extId = addInternalExternalizer(new ClusterListenerRemoveCallable.Externalizer(), extId);
      extId = addInternalExternalizer(new ClusterListenerReplicateCallable.Externalizer(), extId);
      extId = addInternalExternalizer(new CollectionKeyFilter.Externalizer(), extId);
      extId = addInternalExternalizer(new DefaultConsistentHash.Externalizer(), extId);
      extId = addInternalExternalizer(new DeltaCompositeKey.DeltaCompositeKeyExternalizer(), extId);
      extId = addInternalExternalizer(new DldGlobalTransaction.Externalizer(), extId);
      extId = addInternalExternalizer(new DoubleSummaryStatisticsExternalizer(), extId);
      extId = addInternalExternalizer(new EmbeddedMetadata.Externalizer(), extId);
      extId = addInternalExternalizer(new EntryViews.ReadWriteSnapshotViewExternalizer(), extId);
      extId = addInternalExternalizer(new EnumSetExternalizer(), extId);
      extId = addInternalExternalizer(new EquivalenceExternalizer(), extId);
      extId = addInternalExternalizer(new ExceptionResponse.Externalizer(), extId);
      extId = addInternalExternalizer(new Flag.Externalizer(), extId);
      extId = addInternalExternalizer(new GlobalTransaction.Externalizer(), extId);
      extId = addInternalExternalizer(new KeyFilterAsCacheEventFilter.Externalizer(), extId);
      extId = addInternalExternalizer(new KeyFilterAsKeyValueFilter.Externalizer(), extId);
      extId = addInternalExternalizer(new KeyValueFilterAsCacheEventFilter.Externalizer(), extId);
      extId = addInternalExternalizer(new ImmortalCacheEntry.Externalizer(), extId);
      extId = addInternalExternalizer(new ImmortalCacheValue.Externalizer(), extId);
      extId = addInternalExternalizer(new Immutables.ImmutableMapWrapperExternalizer(), extId);
      extId = addInternalExternalizer(new Immutables.ImmutableSetWrapperExternalizer(), extId);
      extId = addInternalExternalizer(new InDoubtTxInfoImpl.Externalizer(), extId);
      extId = addInternalExternalizer(new IntermediateOperationExternalizer(), extId);
      extId = addInternalExternalizer(new InternalMetadataImpl.Externalizer(), extId);
      extId = addInternalExternalizer(new IntSummaryStatisticsExternalizer(), extId);
      extId = addInternalExternalizer(new JGroupsAddress.Externalizer(), extId);
      extId = addInternalExternalizer(new JGroupsTopologyAwareAddress.Externalizer(), extId);
      extId = addInternalExternalizer(new ListExternalizer(), extId);
      extId = addInternalExternalizer(new LongSummaryStatisticsExternalizer(), extId);
      extId = addInternalExternalizer(new KeyValuePair.Externalizer(), extId);
      extId = addInternalExternalizer(new ManagerStatusResponse.Externalizer(), extId);
      extId = addInternalExternalizer(new MapExternalizer(), extId);
      extId = addInternalExternalizer(new MarshallableFunctionExternalizers.ConstantLambdaExternalizer(), extId);
      extId = addInternalExternalizer(new MarshallableFunctionExternalizers.LambdaWithMetasExternalizer(), extId);
      extId = addInternalExternalizer(new MarshallableFunctionExternalizers.SetValueIfEqualsReturnBooleanExternalizer(), extId);
      extId = addInternalExternalizer(new MarshalledEntryImpl.Externalizer(marshaller), extId);
      extId = addInternalExternalizer(new MarshalledValue.Externalizer(marshaller), extId);
      extId = addInternalExternalizer(new MetadataImmortalCacheEntry.Externalizer(), extId);
      extId = addInternalExternalizer(new MetadataImmortalCacheValue.Externalizer(), extId);
      extId = addInternalExternalizer(new MetadataMortalCacheEntry.Externalizer(), extId);
      extId = addInternalExternalizer(new MetadataMortalCacheValue.Externalizer(), extId);
      extId = addInternalExternalizer(new MetadataTransientMortalCacheEntry.Externalizer(), extId);
      extId = addInternalExternalizer(new MetaParamExternalizers.LifespanExternalizer(), extId);
      extId = addInternalExternalizer(new MetaParamExternalizers.EntryVersionParamExternalizer(), extId);
      extId = addInternalExternalizer(new MetaParamExternalizers.NumericEntryVersionExternalizer(), extId);
      extId = addInternalExternalizer(new MetaParams.Externalizer(), extId);
      extId = addInternalExternalizer(new MetaParamsInternalMetadata.Externalizer(), extId);
      extId = addInternalExternalizer(new MIMECacheEntry.Externalizer(), extId); // new
      extId = addInternalExternalizer(new MortalCacheEntry.Externalizer(), extId);
      extId = addInternalExternalizer(new MortalCacheValue.Externalizer(), extId);
      extId = addInternalExternalizer(new MultiClusterEventCallable.Externalizer(), extId);
      extId = addInternalExternalizer(new MurmurHash3.Externalizer(), extId);
      extId = addInternalExternalizer(new NumericVersion.Externalizer(), extId);
      extId = addInternalExternalizer(new OptionalExternalizer(), extId);
      extId = addInternalExternalizer(new PersistentUUID.Externalizer(), extId);
      extId = addInternalExternalizer(new PutOperation.Externalizer(), extId);
      extId = addInternalExternalizer(new QueueExternalizers(), extId);
      extId = addInternalExternalizer(new RecoveryAwareDldGlobalTransaction.Externalizer(), extId);
      extId = addInternalExternalizer(new RecoveryAwareGlobalTransaction.Externalizer(), extId);
      extId = addInternalExternalizer(new RemoveOperation.Externalizer(), extId);
      extId = addInternalExternalizer(new ReplicatedConsistentHash.Externalizer(), extId);
      extId = addInternalExternalizer(new SerializableXid.XidExternalizer(), extId);
      extId = addInternalExternalizer(new SetExternalizer(), extId);
      extId = addInternalExternalizer(new SimpleClusteredVersion.Externalizer(), extId);
      extId = addInternalExternalizer(new SingletonListExternalizer(), extId);
      extId = addInternalExternalizer(new StateChunk.Externalizer(), extId);
      extId = addInternalExternalizer(new StreamMarshalling.StreamMarshallingExternalizer(), extId);
      extId = addInternalExternalizer(new SuccessfulResponse.Externalizer(), extId);
      extId = addInternalExternalizer(new SyncConsistentHashFactory.Externalizer(), extId);
      extId = addInternalExternalizer(new SyncReplicatedConsistentHashFactory.Externalizer(), extId);
      extId = addInternalExternalizer(new TerminalOperationExternalizer(), extId);
      extId = addInternalExternalizer(new TopologyAwareSyncConsistentHashFactory.Externalizer(), extId);
      extId = addInternalExternalizer(new TransactionInfo.Externalizer(), extId);
      extId = addInternalExternalizer(new TransientCacheEntry.Externalizer(), extId);
      extId = addInternalExternalizer(new TransientCacheValue.Externalizer(), extId);
      extId = addInternalExternalizer(new TransientMortalCacheEntry.Externalizer(), extId);
      extId = addInternalExternalizer(new TransientMortalCacheValue.Externalizer(), extId);
      extId = addInternalExternalizer(new UnsuccessfulResponse.Externalizer(), extId);
      extId = addInternalExternalizer(new UnsureResponse.Externalizer(), extId);
      extId = addInternalExternalizer(new UuidExternalizer(), extId);
      extId = addInternalExternalizer(new XSiteState.XSiteStateExternalizer(), extId);

      // ADD NEW INTERNAL EXTERNALIZERS HERE!
   }

   private int addInternalExternalizer(AdvancedExternalizer ext, int extId) {
      internalExts[extId] = ext;

      Set<Class<?>> subTypes = ext.getTypeClasses();
      for (Class<?> subType : subTypes)
         internalExtIds.put(subType, extId);

      return extId + 1;
   }

   private void loadForeignMarshallables(GlobalConfiguration globalCfg) {
      log.trace("Loading user defined externalizers");
      for (Map.Entry<Integer, AdvancedExternalizer<?>> config : globalCfg.serialization().advancedExternalizers().entrySet()) {
         AdvancedExternalizer ext = config.getValue();

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


         predefExts.put(id, ext);

         Set<Class> subTypes = ext.getTypeClasses();
         for (Class<?> subType : subTypes)
            predefExtIds.put(subType, id);
      }
   }

   enum MarshallableType {

      PRIMITIVE,
      INTERNAL,
      PREDEFINED,
      ANNOTATED,
      NOT_MARSHALLABLE;

      boolean isMarshallable() {
         return this != NOT_MARSHALLABLE;
      }

   }

}
