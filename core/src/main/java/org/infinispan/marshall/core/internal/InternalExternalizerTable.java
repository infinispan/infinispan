package org.infinispan.marshall.core.internal;

import org.infinispan.atomic.impl.AtomicHashMap;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeFunctionWith;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Immutables;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.context.Flag;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.marshall.exts.ArrayExternalizers;
import org.infinispan.marshall.exts.CacheRpcCommandExternalizer;
import org.infinispan.marshall.exts.EnumSetExternalizer;
import org.infinispan.marshall.exts.ListExternalizer;
import org.infinispan.marshall.exts.MapExternalizer;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.marshall.exts.SetExternalizer;
import org.infinispan.marshall.exts.SingletonListExternalizer;
import org.infinispan.marshall.exts.UuidExternalizer;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.remoting.MIMECacheEntry;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

   private volatile boolean started;

   InternalExternalizerTable(Encoding enc, GlobalComponentRegistry gcr, RemoteCommandsFactory cmdFactory) {
      this.primitives = new PrimitiveExternalizer(enc);
      this.gcr = gcr;
      this.cmdFactory = cmdFactory;
   }

   void start() {
      loadInternalMarshallables();
      // loadForeignMarshallables(gcr.getGlobalConfiguration());
      started = true;
      if (trace) {
         log.tracef("Constant object table was started and contains these externalizer readers: %s", readers);
         log.tracef("The externalizer writers collection contains: %s", writers);
      }
   }

   void stop() {
      started = false;
      writers.clear();
      readers.clear();
      log.trace("Externalizer reader and writer maps have been cleared and constant object table was stopped");
   }

   <T> Externalizer<T> findWriteExternalizer(Object obj, ObjectOutput out) throws IOException {
      Class<?> clazz = checkStarted(obj);
      Externalizer<T> ext;
      if (clazz == null || primitives.getTypeClasses().contains(clazz)) {
         out.writeByte(InternalIds.PRIMITIVE);
         ext = (Externalizer<T>) primitives;
      } else {
         ext = (Externalizer<T>) writers.get(clazz);
         if (ext != null) {
            out.writeByte(InternalIds.NON_PRIMITIVE);
            out.writeByte(((AdvancedExternalizer<T>) ext).getId());
         } else if ((ext = findAnnotatedExternalizer(clazz)) != null) {
            out.writeByte(InternalIds.ANNOTATED);
            out.writeObject(ext.getClass());
         } else {
            out.writeByte(InternalIds.EXTERNAL);
         }
      }
      return ext;
   }

   private Class<?> checkStarted(Object obj) {
      if (!started) {
         // TODO: Update Log.java eventually (not doing yet to avoid need to rebase)
         // throw log.externalizerTableStopped(clazz.getName());
         String className = obj == null ? "null" : obj.getClass().getName();
         throw new CacheException("Cache manager is shutting down, so type write externalizer for type="
               + className + " cannot be resolved");
      }

      return obj == null ? null : obj.getClass();
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
      ReplicableCommandExternalizer ext = new ReplicableCommandExternalizer(cmdFactory, gcr);
      addInternalExternalizer(ext);

      addInternalExternalizer(new ArrayExternalizers.ListArray());
      addInternalExternalizer(new AtomicHashMap.Externalizer());
      addInternalExternalizer(new CacheRpcCommandExternalizer(gcr, ext));
      addInternalExternalizer(new EmbeddedMetadata.Externalizer());
      addInternalExternalizer(new EnumSetExternalizer());
      addInternalExternalizer(new ExceptionResponse.Externalizer());
      addInternalExternalizer(new Flag.Externalizer());
      addInternalExternalizer(new GlobalTransaction.Externalizer());
      addInternalExternalizer(new ImmortalCacheEntry.Externalizer());
      addInternalExternalizer(new ImmortalCacheValue.Externalizer());
      addInternalExternalizer(new Immutables.ImmutableMapWrapperExternalizer());
      addInternalExternalizer(new InternalMetadataImpl.Externalizer());
      addInternalExternalizer(new JGroupsAddress.Externalizer());
      addInternalExternalizer(new ListExternalizer());
      addInternalExternalizer(new KeyValuePair.Externalizer());
      addInternalExternalizer(new MapExternalizer());
      addInternalExternalizer(new MarshalledValue.Externalizer(gcr.getComponent(StreamingMarshaller.class)));
      addInternalExternalizer(new MIMECacheEntry.Externalizer()); // new
      addInternalExternalizer(new MortalCacheEntry.Externalizer());
      addInternalExternalizer(new MortalCacheValue.Externalizer());
      addInternalExternalizer(new SetExternalizer());
      addInternalExternalizer(new SingletonListExternalizer());
      addInternalExternalizer(new TransientCacheEntry.Externalizer());
      addInternalExternalizer(new TransientCacheValue.Externalizer());
      addInternalExternalizer(new TransientMortalCacheEntry.Externalizer());
      addInternalExternalizer(new TransientMortalCacheValue.Externalizer());
      addInternalExternalizer(new UnsuccessfulResponse.Externalizer());
      addInternalExternalizer(new UuidExternalizer()); // new
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

}
