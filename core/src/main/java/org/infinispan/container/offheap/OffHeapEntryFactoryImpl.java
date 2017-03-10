package org.infinispan.container.offheap;

import java.io.IOException;

import org.infinispan.commands.InvocationRecord;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.TimeService;

/**
 * Factory that can create CacheEntry instances from off-heap memory.
 *
 * @author wburns
 * @since 9.0
 */
public class OffHeapEntryFactoryImpl implements OffHeapEntryFactory {
   private static final OffHeapMemory MEMORY = OffHeapMemory.INSTANCE;
   private static final byte[] EMPTY_BYTES = new byte[0];

   @Inject private StreamingMarshaller marshaller;
   @Inject private OffHeapMemoryAllocator allocator;
   @Inject private TimeService timeService;
   @Inject private InternalEntryFactory internalEntryFactory;
   @Inject private Configuration configuration;

   private boolean evictionEnabled;
   private boolean transactional;

   // If custom than we just store the metadata as is (no other bits should be used)
   private static final int CUSTOM = 1;
   // Version can be set with any combination of the following types
   private static final int HAS_VERSION = 2;
   // Only one of the following should ever be set
   private static final int MORTAL = 1 << 2;
   private static final int TRANSIENT = 1 << 3;
   private static final int HAS_INVOCATIONS = 1 << 4;
   private static final int EMBEDDED_TYPE_MASK = TRANSIENT | MORTAL;

   /**
    * HEADER is composed of type (byte), hashCode (int), keyLength (int), valueLength (int)
    * Note that metadata is not included as this is now optional
    */
   private static final int HEADER_LENGTH = 1 + 4 + 4 + 4;

   @Start
   public void start() {
      this.evictionEnabled = configuration.memory().isEvictionEnabled();
      this.transactional = configuration.transaction().transactionMode().isTransactional();
   }

   /**
    * Create an entry off-heap.  The first 8 bytes will always be 0, reserved for a future reference to another entry
    * @param key the key to use
    * @param value the value to use
    * @param metadata the metadata to use
    * @return the address of the entry created off heap
    */
   @Override
   public long create(WrappedBytes key, WrappedBytes value, Metadata metadata) {
      byte type = 0;
      boolean shouldWriteMetadataSize = false;
      byte[] metadataBytes;
      if (metadata instanceof EmbeddedMetadata) {
         // regular
      } else if (metadata instanceof MetaParamsInternalMetadata && !((MetaParamsInternalMetadata) metadata).isCustom()) {
         // metaparams without any custom type
      } else {
         type = CUSTOM;
      }
      if (type != CUSTOM) {
         int versionInvocationsLength = 0;
         EntryVersion version = metadata.version();
         byte[] versionBytes;
         if (version != null) {
            type |= HAS_VERSION;
            shouldWriteMetadataSize = true;
            try {
               versionBytes = marshaller.objectToByteBuffer(version);
               versionInvocationsLength += versionBytes.length;
            } catch (IOException | InterruptedException e) {
               throw new CacheException(e);
            }
         } else {
            versionBytes = EMPTY_BYTES;
         }
         ByteBuffer invocationBytes;
         if (metadata.lastInvocation() != null) {
            type |= HAS_INVOCATIONS;
            shouldWriteMetadataSize = true;
            try {
               invocationBytes = metadata.lastInvocation().toByteBuffer(marshaller);
               versionInvocationsLength += invocationBytes.getLength();
            } catch (IOException e) {
               throw new CacheException(e);
            }
         } else {
            invocationBytes = null;
         }

         if (type == (HAS_INVOCATIONS | HAS_VERSION)) {
            versionInvocationsLength += 4;
         }

         long lifespan = metadata.lifespan();
         long maxIdle = metadata.maxIdle();

         if (lifespan < 0 && maxIdle < 0) {
            if ((type & HAS_VERSION) == 0) {
               metadataBytes = invocationBytes == null ? EMPTY_BYTES : invocationBytes.toBytes();
            } else if ((type & HAS_INVOCATIONS) == 0) {
               metadataBytes = versionBytes;
            } else {
               metadataBytes = new byte[versionInvocationsLength];
               Bits.putInt(metadataBytes, 0, versionBytes.length);
               System.arraycopy(versionBytes, 0, metadataBytes, 4, versionBytes.length);
               invocationBytes.copyTo(metadataBytes, 4 + versionBytes.length);
            }
         } else if (lifespan > -1 && maxIdle < 0) {
            type |= MORTAL;
            metadataBytes = new byte[16 + versionInvocationsLength];
            Bits.putLong(metadataBytes, 0, lifespan);
            Bits.putLong(metadataBytes, 8, timeService.wallClockTime());
            putVersionsInvocations(metadataBytes, 16, type, versionBytes, invocationBytes);
         } else if (lifespan < 0 && maxIdle > -1) {
            type |= TRANSIENT;
            metadataBytes = new byte[16 + versionInvocationsLength];
            Bits.putLong(metadataBytes, 0, maxIdle);
            Bits.putLong(metadataBytes, 8, timeService.wallClockTime());
            putVersionsInvocations(metadataBytes, 16, type, versionBytes, invocationBytes);
         } else {
            type |= TRANSIENT | MORTAL;
            metadataBytes = new byte[32 + versionInvocationsLength];
            Bits.putLong(metadataBytes, 0, maxIdle);
            Bits.putLong(metadataBytes, 8, lifespan);
            long time = timeService.wallClockTime();
            Bits.putLong(metadataBytes, 16, time);
            Bits.putLong(metadataBytes, 24, time);
            putVersionsInvocations(metadataBytes, 32, type, versionBytes, invocationBytes);
         }
      } else {
         shouldWriteMetadataSize = true;
         try {
            metadataBytes = marshaller.objectToByteBuffer(metadata);
         } catch (IOException | InterruptedException e) {
            throw new CacheException(e);
         }
      }

      int keySize = key.getLength();
      int metadataSize = metadataBytes.length;
      int valueSize = value == null ? 0 : value.getLength();

      // Eviction requires 2 additional pointers at the beginning
      int offset = evictionEnabled ? 16 : 0;
      // Next 8 is for linked pointer to next address
      long totalSize = offset + 8 + HEADER_LENGTH +
            // If the type has a version or is custom we have to add 4 more bytes for an int to include that
            (shouldWriteMetadataSize ? 4 : 0)
            + keySize + metadataSize + valueSize;
      long memoryAddress = allocator.allocate(totalSize);

      // Write the empty linked address pointer first
      MEMORY.putLong(memoryAddress, offset, 0);
      offset += 8;

      MEMORY.putByte(memoryAddress, offset, type);
      offset += 1;
      MEMORY.putInt(memoryAddress, offset, key.hashCode());
      offset += 4;
      MEMORY.putInt(memoryAddress, offset, key.getLength());
      offset += 4;
      if (shouldWriteMetadataSize) {
         MEMORY.putInt(memoryAddress, offset, metadataBytes.length);
         offset += 4;
      }
      MEMORY.putInt(memoryAddress, offset, valueSize);
      offset += 4;

      MEMORY.putBytes(key.getBytes(), key.backArrayOffset(), memoryAddress, offset, keySize);
      offset += keySize;

      MEMORY.putBytes(metadataBytes, 0, memoryAddress, offset, metadataSize);
      offset += metadataSize;

      if (value != null) {
         MEMORY.putBytes(value.getBytes(), value.backArrayOffset(), memoryAddress, offset, valueSize);
         offset += valueSize;
      }

      assert offset == totalSize;

      return memoryAddress;
   }

   private void putVersionsInvocations(byte[] metadataBytes, int offset, byte type, byte[] versionBytes, ByteBuffer invocationBytes) {
      int hasBoth = HAS_VERSION | HAS_INVOCATIONS;
      if ((type & hasBoth) == hasBoth) {
         Bits.putInt(metadataBytes, offset, versionBytes.length);
         System.arraycopy(versionBytes, 0, metadataBytes, offset + 4, versionBytes.length);
         invocationBytes.copyTo(metadataBytes, offset + 4 + versionBytes.length);
      } else if ((type & HAS_VERSION) != 0) {
         System.arraycopy(versionBytes, 0, metadataBytes, offset, versionBytes.length);
      } else if ((type & HAS_INVOCATIONS) != 0) {
         invocationBytes.copyTo(metadataBytes, offset);
      }
   }

   @Override
   public long getSize(long entryAddress) {
      int headerOffset = evictionEnabled ? 24 : 8;

      byte type = MEMORY.getByte(entryAddress, headerOffset);
      headerOffset++;
      // Skip the hashCode
      headerOffset += 4;
      int keyLength = MEMORY.getInt(entryAddress, headerOffset);
      headerOffset += 4;
      int metadataLength;
      if ((type & (CUSTOM | HAS_VERSION)) != 0) {
         metadataLength = MEMORY.getInt(entryAddress, headerOffset);
         headerOffset += 4;
      } else {
         metadataLength = 0;
      }

      int valueLength = MEMORY.getInt(entryAddress, headerOffset);
      headerOffset += 4;

      return UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(headerOffset + keyLength + metadataLength + valueLength);
   }

   @Override
   public long getNext(long entryAddress) {
      return MEMORY.getLong(entryAddress, evictionEnabled ? 16 : 0);
   }

   @Override
   public void setNext(long entryAddress, long value) {
      MEMORY.putLong(entryAddress, evictionEnabled ? 16 : 0, value);
   }

   @Override
   public int getHashCode(long entryAddress) {
      // 16 bytes for eviction if needed (optional)
      // 8 bytes for linked pointer
      // 1 for type
      int headerOffset = evictionEnabled ? 25 : 9;
      return MEMORY.getInt(entryAddress, headerOffset);
   }

   /**
    * Assumes the address doesn't contain the linked pointer at the beginning
    * @param address the address to read the entry from
    * @return the entry at the memory location
    */
   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> fromMemory(long address) {
      // 16 bytes for eviction if needed (optional)
      // 8 bytes for linked pointer
      int offset = evictionEnabled ? 24 : 8;

      byte metadataType = MEMORY.getByte(address, offset);
      offset += 1;
      int hashCode = MEMORY.getInt(address, offset);
      offset += 4;
      byte[] keyBytes = new byte[MEMORY.getInt(address, offset)];
      offset += 4;

      byte[] metadataBytes;
      switch (metadataType) {
         case 0:
            metadataBytes = EMPTY_BYTES;
            break;
         case MORTAL:
            metadataBytes = new byte[16];
            break;
         case TRANSIENT:
            metadataBytes = new byte[16];
            break;
         case TRANSIENT | MORTAL:
            metadataBytes = new byte[32];
            break;
         default:
            // This means we had CUSTOM or HAS_VERSION so we have to read it all
            metadataBytes = new byte[MEMORY.getInt(address, offset)];
            offset += 4;
      }

      // TODO: when this is a tombstone we don't need to write value length
      int valueLength = MEMORY.getInt(address, offset);
      byte[] valueBytes = valueLength == 0 ? null : new byte[valueLength];
      offset += 4;

      MEMORY.getBytes(address, offset, keyBytes, 0, keyBytes.length);
      offset += keyBytes.length;
      MEMORY.getBytes(address, offset, metadataBytes, 0, metadataBytes.length);
      offset += metadataBytes.length;

      WrappedByteArray value = null;
      if (valueLength != 0) {
         MEMORY.getBytes(address, offset, valueBytes, 0, valueBytes.length);
         value = new WrappedByteArray(valueBytes);
      }

      Metadata metadata;
      // This is a custom metadata
      if ((metadataType & CUSTOM) == CUSTOM) {
         try {
            metadata = (Metadata) marshaller.objectFromByteBuffer(metadataBytes);
         } catch (IOException | ClassNotFoundException e) {
            throw new CacheException(e);
         }
         return internalEntryFactory.create(new WrappedByteArray(keyBytes, hashCode), value, metadata);
      } else {
         long lifespan;
         long maxIdle;
         long created;
         long lastUsed;
         offset = 0;
         boolean hasVersion = (metadataType & HAS_VERSION) == HAS_VERSION;
         boolean hasInvocations = (metadataType & HAS_INVOCATIONS) == HAS_INVOCATIONS;
         // Ignore CUSTOM and VERSION to find type
         switch (metadataType & EMBEDDED_TYPE_MASK) {
            case 0:
               lifespan = -1;
               maxIdle = -1;
               created = -1;
               lastUsed = -1;
               break;
            case MORTAL:
               maxIdle = -1;
               lifespan = Bits.getLong(metadataBytes, offset);
               created = Bits.getLong(metadataBytes, offset += 8);
               lastUsed = -1;
               break;
            case TRANSIENT:
               lifespan = -1;
               maxIdle = Bits.getLong(metadataBytes, offset);
               created = -1;
               lastUsed = Bits.getLong(metadataBytes, offset += 8);
               break;
            case TRANSIENT | MORTAL:
               lifespan = Bits.getLong(metadataBytes, offset);
               maxIdle = Bits.getLong(metadataBytes, offset += 8);
               created = Bits.getLong(metadataBytes, offset += 8);
               lastUsed = Bits.getLong(metadataBytes, offset += 8);
               break;
            default:
               throw new IllegalArgumentException("Unsupported type: " + metadataType);
         }

         try {
            if (hasVersion) {
               if (hasInvocations) {
                  int versionLength = Bits.getInt(metadataBytes, offset);
                  EntryVersion version = (EntryVersion) marshaller.objectFromByteBuffer(metadataBytes, offset += 4, versionLength);
                  InvocationRecord invocations = InvocationRecord.fromBytes(marshaller, metadataBytes, offset += versionLength, metadataBytes.length - offset);
                  metadata = new EmbeddedMetadata.Builder().version(version).invocations(invocations).lifespan(lifespan).maxIdle(maxIdle).build();
                  return internalEntryFactory.create(new WrappedByteArray(keyBytes, hashCode),
                        value, metadata, created, lifespan, lastUsed, maxIdle);
               } else {
                  EntryVersion version = (EntryVersion) marshaller.objectFromByteBuffer(metadataBytes, offset, metadataBytes.length - offset);
                  return internalEntryFactory.create(new WrappedByteArray(keyBytes, hashCode),
                        value, version, created, lifespan, lastUsed, maxIdle);
               }
            } else if (hasInvocations) {
               InvocationRecord invocations = InvocationRecord.fromBytes(marshaller, metadataBytes, offset, metadataBytes.length - offset);
               metadata = new EmbeddedMetadata.Builder().invocations(invocations).lifespan(lifespan).maxIdle(maxIdle).build();
               return internalEntryFactory.create(new WrappedByteArray(keyBytes, hashCode),
                     value, metadata, created, lifespan, lastUsed, maxIdle);
            } else {
               return internalEntryFactory.create(new WrappedByteArray(keyBytes, hashCode),
                     value, (Metadata) null, created, lifespan, lastUsed, maxIdle);
            }
         } catch (IOException | ClassNotFoundException e) {
            throw new CacheException("Metadata: " + Util.hexDump(metadataBytes), e);
         }
      }
   }

   /**
    * Assumes the address points to the entry excluding the pointer reference at the beginning
    * @param address the address of an entry to read
    * @param wrappedBytes the key to check if it equals
    * @return whether the key and address are equal
    */
   @Override
   public boolean equalsKey(long address, WrappedBytes wrappedBytes) {
      // 16 bytes for eviction if needed (optional)
      // 8 bytes for linked pointer
      int headerOffset = evictionEnabled ? 24 : 8;
      byte type = MEMORY.getByte(address, headerOffset);
      headerOffset++;
      // First if hashCode doesn't match then the key can't be equal
      int hashCode = wrappedBytes.hashCode();
      if (hashCode != MEMORY.getInt(address, headerOffset)) {
         return false;
      }
      headerOffset += 4;
      // If the length of the key is not the same it can't match either!
      int keyLength = MEMORY.getInt(address, headerOffset);
      if (keyLength != wrappedBytes.getLength()) {
         return false;
      }
      headerOffset += 4;
      if (requiresMetadataSize(type)) {
         headerOffset += 4;
      }
      // This is for the value size which we don't need to read
      headerOffset += 4;
      // Finally read each byte individually so we don't have to copy them into a byte[]
      for (int i = 0; i < keyLength; i++) {
         byte b = MEMORY.getByte(address, headerOffset + i);
         if (b != wrappedBytes.getByte(i))
            return false;
      }

      return true;
   }

   /**
    * Returns whether entry is expired.
    * @param address the address of the entry to check
    * @return {@code true} if the entry is expired, {@code false} otherwise
    */
   @Override
   public boolean isExpired(long address) {
      // 16 bytes for eviction if needed (optional)
      // 8 bytes for linked pointer
      int offset = evictionEnabled ? 24 : 8;

      byte metadataType = MEMORY.getByte(address, offset);
      if ((metadataType & EMBEDDED_TYPE_MASK) == 0) {
         return false;
      }
      // type
      offset += 1;
      // hashCode
      offset += 4;
      // key length
      int keyLength = MEMORY.getInt(address, offset);
      offset += 4;

      long now = timeService.wallClockTime();

      byte[] metadataBytes;
      if ((metadataType & CUSTOM) == CUSTOM) {
         // TODO: this needs to be fixed in ISPN-8539
         return false;
//         int metadataLength = MEMORY.getInt(address, offset);
//         metadataBytes = new byte[metadataLength];
//
//         // value and keyLength
//         offset += 4 + keyLength;
//
//         MEMORY.getBytes(address, offset, metadataBytes, 0, metadataBytes.length);
//
//         Metadata metadata;
//         try {
//            metadata = (Metadata) marshaller.objectFromByteBuffer(metadataBytes);
//            // TODO: custom metadata is not implemented properly for expiration
//            return false;
//         } catch (IOException | ClassNotFoundException e) {
//            throw new CacheException(e);
//         }
      } else {
         // value and keyLength
         offset += 4 + keyLength;

         // If it has version that means we wrote the size as well which goes after key length
         if ((metadataType & HAS_VERSION) != 0) {
            offset += 4;
         }

         switch (metadataType & 0xFC) {
            case MORTAL:
               metadataBytes = new byte[16];
               MEMORY.getBytes(address, offset, metadataBytes, 0, metadataBytes.length);
               return ExpiryHelper.isExpiredMortal(Bits.getLong(metadataBytes, 0), Bits.getLong(metadataBytes, 8), now);
            case TRANSIENT:
               metadataBytes = new byte[16];
               MEMORY.getBytes(address, offset, metadataBytes, 0, metadataBytes.length);
               return ExpiryHelper.isExpiredTransient(Bits.getLong(metadataBytes, 0), Bits.getLong(metadataBytes, 8), now);
            case TRANSIENT | MORTAL:
               metadataBytes = new byte[32];
               MEMORY.getBytes(address, offset, metadataBytes, 0, metadataBytes.length);
               long lifespan = Bits.getLong(metadataBytes, 0);
               long maxIdle = Bits.getLong(metadataBytes, 8);
               long created = Bits.getLong(metadataBytes, 16);
               long lastUsed = Bits.getLong(metadataBytes, 24);
               return ExpiryHelper.isExpiredTransientMortal(maxIdle, lastUsed, lifespan, created, now);
            default:
               return false;
         }
      }
   }

   static private boolean requiresMetadataSize(byte type) {
      return (type & (CUSTOM | HAS_VERSION | HAS_INVOCATIONS)) != 0;
   }

   @Override
   public long calculateSize(WrappedBytes key, WrappedBytes value, Metadata metadata) {
      long totalSize = evictionEnabled ? 24 : 8;
      totalSize += HEADER_LENGTH;
      totalSize += key.getLength() + value.getLength();
      if (transactional && metadata != null && metadata.lastInvocation() != null) {
         // estimate the size without invocations as these are kept only in the context
         metadata = metadata.builder().noInvocations().build();
      }
      long metadataSize;
      if (metadata instanceof EmbeddedMetadata ||
            metadata instanceof MetaParamsInternalMetadata && !((MetaParamsInternalMetadata) metadata).isCustom()) {
         metadataSize = 0;
         EntryVersion version = metadata.version();
         if (version != null) {
            try {
               metadataSize += marshaller.objectToByteBuffer(version).length;
            } catch (IOException | InterruptedException e) {
               throw new CacheException(e);
            }
            // We have to write the size of the version
            metadataSize += 4;
         }
         if (metadata.maxIdle() >= 0) {
            metadataSize += 16;
         }
         if (metadata.lifespan() >= 0) {
            metadataSize += 16;
         }
         if (metadata.lastInvocation() != null) {
            try {
               metadataSize += metadata.lastInvocation().toByteBuffer(marshaller).getLength();
            } catch (IOException e) {
               throw new CacheException(e);
            }
            if (version != null) {
               metadataSize += 4;
            }
         }
      } else {
         // We have to write the size of the metadata object
         try {
            metadataSize = 4 + marshaller.objectToByteBuffer(metadata).length;
         } catch (IOException | InterruptedException e) {
            throw new CacheException(e);
         }
      }
      return UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(totalSize + metadataSize);
   }
}
