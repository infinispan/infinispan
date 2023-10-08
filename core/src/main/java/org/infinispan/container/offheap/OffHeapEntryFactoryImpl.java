package org.infinispan.container.offheap;

import static org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator.estimateSizeOverhead;
import static org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator.offHeapEntrySize;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.spi.OffHeapMemory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * Factory that can create CacheEntry instances from off-heap memory.
 *
 * @author wburns
 * @since 9.0
 */
@Scope(Scopes.NAMED_CACHE)
public class OffHeapEntryFactoryImpl implements OffHeapEntryFactory {
   private static final OffHeapMemory MEMORY = org.infinispan.commons.jdkspecific.OffHeapMemory.getInstance();

   @Inject @ComponentName(KnownComponentNames.INTERNAL_MARSHALLER)
   Marshaller marshaller;
   @Inject OffHeapMemoryAllocator allocator;
   @Inject TimeService timeService;
   @Inject InternalEntryFactory internalEntryFactory;
   @Inject Configuration configuration;

   private boolean evictionEnabled;

   // If custom than we just store the metadata as is (no other bits should be used)
   private static final byte CUSTOM = 1;
   // Version can be set with any combination of the following types
   private static final byte HAS_VERSION = 2;
   // Only one of the following 4 should ever be set
   // It should be possible to reuse this bit for something else if needed as the absence of the other three
   // can imply it is immortal
   private static final byte IMMORTAL = 1 << 2;
   private static final byte MORTAL = 1 << 3;
   private static final byte TRANSIENT = 1 << 4;
   private static final byte TRANSIENT_MORTAL = 1 << 5;

   private static final byte EXPIRATION_TYPES = IMMORTAL | MORTAL | TRANSIENT | TRANSIENT_MORTAL;

   // Whether this entry has private metadata or not
   private static final byte HAS_PRIVATE_METADATA = 1 << 6;

   /**
    * HEADER is composed of type (byte), hashCode (int), keyLength (int), valueLength (int)
    * Note that metadata is not included as this is now optional
    */
   static final int HEADER_LENGTH = 1 + 4 + 4 + 4;

   @Start
   public void start() {
      this.evictionEnabled = configuration.memory().isEvictionEnabled();
   }

   @Override
   public long create(WrappedBytes key, int hashCode, InternalCacheEntry<WrappedBytes, WrappedBytes> ice) {
      byte type;
      boolean shouldWriteMetadataSize = false;
      byte[] metadataBytes;
      Metadata metadata = ice.getMetadata();
      if (metadata instanceof EmbeddedMetadata) {
         EntryVersion version = metadata.version();
         byte[] versionBytes;
         if (version != null) {
            type = HAS_VERSION;
            shouldWriteMetadataSize = true;
            try {
               versionBytes = marshaller.objectToByteBuffer(version);
            } catch (IOException | InterruptedException e) {
               throw new CacheException(e);
            }
         } else {
            type = 0;
            versionBytes = Util.EMPTY_BYTE_ARRAY;
         }

         long lifespan = metadata.lifespan();
         long maxIdle = metadata.maxIdle();

         if (lifespan < 0 && maxIdle < 0) {
            type |= IMMORTAL;
            metadataBytes = versionBytes;
         } else if (lifespan > -1 && maxIdle < 0) {
            type |= MORTAL;
            metadataBytes = new byte[16 + versionBytes.length];
            Bits.putLong(metadataBytes, 0, lifespan);
            Bits.putLong(metadataBytes, 8, ice.getCreated());
            System.arraycopy(versionBytes, 0, metadataBytes, 16, versionBytes.length);
         } else if (lifespan < 0) {
            type |= TRANSIENT;
            metadataBytes = new byte[16 + versionBytes.length];
            Bits.putLong(metadataBytes, 0, maxIdle);
            Bits.putLong(metadataBytes, 8, ice.getLastUsed());
            System.arraycopy(versionBytes, 0, metadataBytes, 16, versionBytes.length);
         } else {
            type |= TRANSIENT_MORTAL;
            metadataBytes = new byte[32 + versionBytes.length];
            Bits.putLong(metadataBytes, 0, lifespan);
            Bits.putLong(metadataBytes, 8, maxIdle);
            Bits.putLong(metadataBytes, 16, ice.getCreated());
            Bits.putLong(metadataBytes, 24, ice.getLastUsed());
            System.arraycopy(versionBytes, 0, metadataBytes, 32, versionBytes.length);
         }
      } else {
         type = CUSTOM;
         shouldWriteMetadataSize = true;
         metadataBytes = marshall(metadata);
      }


      int keySize = key.getLength();
      int metadataSize = metadataBytes.length;
      WrappedBytes value = ice.getValue();
      int valueSize = value != null ? value.getLength() : 0;

      byte[] internalMetadataBytes;
      int internalMetadataSize;
      if (shouldWriteInternalMetadata(ice.getInternalMetadata())) {
         internalMetadataBytes = marshall(ice.getInternalMetadata());
         internalMetadataSize = internalMetadataBytes.length;
         type |= HAS_PRIVATE_METADATA;
      } else {
         internalMetadataBytes = null;
         internalMetadataSize = 0;
      }

      // Eviction requires 2 additional pointers at the beginning
      int offset = evictionEnabled ? 16 : 0;
      long totalSize = offHeapEntrySize(evictionEnabled, shouldWriteMetadataSize, keySize, valueSize, metadataSize, internalMetadataSize);
      long memoryAddress = allocator.allocate(totalSize);

      // Write the empty linked address pointer first
      MEMORY.putLong(memoryAddress, offset, 0);
      offset += 8;

      MEMORY.putByte(memoryAddress, offset, type);
      offset += 1;
      MEMORY.putInt(memoryAddress, offset, hashCode);
      offset += 4;
      MEMORY.putInt(memoryAddress, offset, key.getLength());
      offset += 4;
      if (shouldWriteMetadataSize) {
         MEMORY.putInt(memoryAddress, offset, metadataBytes.length);
         offset += 4;
      }
      MEMORY.putInt(memoryAddress, offset, valueSize);
      offset += 4;

      if (internalMetadataSize > 0) {
         MEMORY.putInt(memoryAddress, offset, internalMetadataSize);
         offset += 4;
      }

      MEMORY.putBytes(key.getBytes(), key.backArrayOffset(), memoryAddress, offset, keySize);
      offset += keySize;

      MEMORY.putBytes(metadataBytes, 0, memoryAddress, offset, metadataSize);
      offset += metadataSize;

      if (valueSize > 0) {
         MEMORY.putBytes(value.getBytes(), value.backArrayOffset(), memoryAddress, offset, valueSize);
         offset += valueSize;
      }

      if (internalMetadataSize > 0) {
         MEMORY.putBytes(internalMetadataBytes, 0, memoryAddress, offset, internalMetadataSize);
         offset += internalMetadataSize;
      }

      assert offset == totalSize;

      return memoryAddress;
   }

   @Override
   public long getSize(long entryAddress, boolean includeAllocationOverhead) {
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
         switch (type & EXPIRATION_TYPES) {
            case IMMORTAL:
               metadataLength = 0;
               break;
            case MORTAL:
            case TRANSIENT:
               metadataLength = 16;
               break;
            case TRANSIENT_MORTAL:
               metadataLength = 32;
               break;
            default:
               throw new IllegalArgumentException("Unsupported type: " + type);
         }
      }

      int valueLength = MEMORY.getInt(entryAddress, headerOffset);
      headerOffset += 4;

      int internalMetadataLength;
      if (requiresInternalMetadataSize(type)) {
         internalMetadataLength = MEMORY.getInt(entryAddress, headerOffset);
         headerOffset += 4;
      } else {
         internalMetadataLength = 0;
      }

      int size = headerOffset + keyLength + metadataLength + valueLength + internalMetadataLength;
      return includeAllocationOverhead ? estimateSizeOverhead(size) : size;
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

   @Override
   public byte[] getKey(long address) {
      // 16 bytes for eviction if needed (optional)
      // 8 bytes for linked pointer
      int offset = evictionEnabled ? 24 : 8;

      byte metadataType = MEMORY.getByte(address, offset);
      offset += 1;
      // Ignore hashCode bytes
      offset += 4;
      byte[] keyBytes = new byte[MEMORY.getInt(address, offset)];
      offset += 4;

      if ((metadataType & (CUSTOM + HAS_VERSION)) != 0) {
         // These have additional 4 bytes for custom metadata or version
         offset += 4;
      }

      // Ignore value bytes
      offset += 4;

      // Ignore internal metadata bytes
      if (requiresInternalMetadataSize(metadataType)) {
         offset += 4;
      }

      // Finally read the bytes and return
      MEMORY.getBytes(address, offset, keyBytes, 0, keyBytes.length);
      return keyBytes;
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
      switch (metadataType & (~HAS_PRIVATE_METADATA)) {
         case IMMORTAL:
            metadataBytes = Util.EMPTY_BYTE_ARRAY;
            break;
         case MORTAL:
         case TRANSIENT:
            metadataBytes = new byte[16];
            break;
         case TRANSIENT_MORTAL:
            metadataBytes = new byte[32];
            break;
         default:
            // This means we had CUSTOM or HAS_VERSION so we have to read it all
            metadataBytes = new byte[MEMORY.getInt(address, offset)];
            offset += 4;
      }

      int valueSize = MEMORY.getInt(address, offset);
      offset += 4;

      int internalMetadataSize;
      if (requiresInternalMetadataSize(metadataType)) {
         internalMetadataSize = MEMORY.getInt(address, offset);
         offset += 4;
      } else {
         internalMetadataSize = 0;
      }

      MEMORY.getBytes(address, offset, keyBytes, 0, keyBytes.length);
      offset += keyBytes.length;
      MEMORY.getBytes(address, offset, metadataBytes, 0, metadataBytes.length);
      offset += metadataBytes.length;

      WrappedBytes valueWrappedBytes;
      if (valueSize > 0) {
         byte[] valueBytes = new byte[valueSize];
         MEMORY.getBytes(address, offset, valueBytes, 0, valueBytes.length);
         offset += valueBytes.length;

         valueWrappedBytes = new WrappedByteArray(valueBytes);
      } else {
         valueWrappedBytes = null;
      }

      PrivateMetadata internalMetadata = PrivateMetadata.empty();
      if (internalMetadataSize > 0) {
         byte[] internalMetadataBytes = new byte[internalMetadataSize];
         MEMORY.getBytes(address, offset, internalMetadataBytes, 0, internalMetadataSize);
         offset += internalMetadataSize;
         internalMetadata = unmarshall(internalMetadataBytes);
      }


      Metadata metadata;
      // This is a custom metadata
      if ((metadataType & CUSTOM) == CUSTOM) {
         metadata = unmarshall(metadataBytes);
         InternalCacheEntry<WrappedBytes, WrappedBytes>  ice= internalEntryFactory.create(new WrappedByteArray(keyBytes, hashCode),
               valueWrappedBytes, metadata);
         ice.setInternalMetadata(internalMetadata);
         return ice;
      } else {
         long lifespan;
         long maxIdle;
         long created;
         long lastUsed;
         offset = 0;
         boolean hasVersion = (metadataType & HAS_VERSION) == HAS_VERSION;
         // Ignore CUSTOM and VERSION to find type
         switch (metadataType & EXPIRATION_TYPES) {
            case IMMORTAL:
               lifespan = -1;
               maxIdle = -1;
               created = -1;
               lastUsed = -1;
               break;
            case MORTAL:
               maxIdle = -1;
               lifespan = Bits.getLong(metadataBytes, offset);
               offset += 8;
               created = Bits.getLong(metadataBytes, offset);
               offset += 8;
               lastUsed = -1;
               break;
            case TRANSIENT:
               lifespan = -1;
               maxIdle = Bits.getLong(metadataBytes, offset);
               offset += 8;
               created = -1;
               lastUsed = Bits.getLong(metadataBytes, offset);
               offset += 8;
               break;
            case TRANSIENT_MORTAL:
               lifespan = Bits.getLong(metadataBytes, offset);
               offset += 8;
               maxIdle = Bits.getLong(metadataBytes, offset);
               offset += 8;
               created = Bits.getLong(metadataBytes, offset);
               offset += 8;
               lastUsed = Bits.getLong(metadataBytes, offset);
               offset += 8;
               break;
            default:
               throw new IllegalArgumentException("Unsupported type: " + metadataType);
         }
         if (hasVersion) {
            try {
               EntryVersion version = (EntryVersion) marshaller.objectFromByteBuffer(metadataBytes, offset,
                     metadataBytes.length - offset);
               InternalCacheEntry<WrappedBytes, WrappedBytes>  ice= internalEntryFactory.create(new WrappedByteArray(keyBytes, hashCode),
                     valueWrappedBytes, version, created, lifespan, lastUsed, maxIdle);
               ice.setInternalMetadata(internalMetadata);
               return ice;
            } catch (IOException | ClassNotFoundException e) {
               throw new CacheException(e);
            }
         } else {
            InternalCacheEntry<WrappedBytes, WrappedBytes>  ice= internalEntryFactory.create(new WrappedByteArray(keyBytes, hashCode),
                  valueWrappedBytes, (Metadata) null, created, lifespan, lastUsed, maxIdle);
            ice.setInternalMetadata(internalMetadata);
            return ice;
         }
      }
   }

   @Override
   public boolean equalsKey(long address, WrappedBytes wrappedBytes, int hashCode) {
      // 16 bytes for eviction if needed (optional)
      // 8 bytes for linked pointer
      int headerOffset = evictionEnabled ? 24 : 8;
      byte type = MEMORY.getByte(address, headerOffset);
      headerOffset++;
      // First if hashCode doesn't match then the key can't be equal
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

      // This is for the internal metadata size which we don't need to read
      if (requiresInternalMetadataSize(type)) {
         headerOffset += 4;
      }
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
      if ((metadataType & IMMORTAL) != 0) {
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

         // internal metadata length (if applicable)
         if (requiresInternalMetadataSize(metadataType)) {
            offset += 4;
         }

         // If it has version that means we wrote the size as well which goes after key length
         if ((metadataType & HAS_VERSION) != 0) {
            offset += 4;
         }

         switch (metadataType & EXPIRATION_TYPES) {
            case MORTAL:
               metadataBytes = new byte[16];
               MEMORY.getBytes(address, offset, metadataBytes, 0, metadataBytes.length);
               return ExpiryHelper.isExpiredMortal(Bits.getLong(metadataBytes, 0), Bits.getLong(metadataBytes, 8), now);
            case TRANSIENT:
               metadataBytes = new byte[16];
               MEMORY.getBytes(address, offset, metadataBytes, 0, metadataBytes.length);
               return ExpiryHelper.isExpiredTransient(Bits.getLong(metadataBytes, 0), Bits.getLong(metadataBytes, 8), now);
            case TRANSIENT_MORTAL:
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
      return (type & (CUSTOM | HAS_VERSION)) != 0;
   }

   static private boolean requiresInternalMetadataSize(byte type) {
      return (type & HAS_PRIVATE_METADATA) == HAS_PRIVATE_METADATA;
   }

   @Override
   public long calculateSize(WrappedBytes key, WrappedBytes value, Metadata metadata, PrivateMetadata internalMetadata) {
      long totalSize = evictionEnabled ? 24 : 8;
      totalSize += HEADER_LENGTH;
      totalSize += key.getLength() + value.getLength();
      long metadataSize = 0;
      if (metadata instanceof EmbeddedMetadata) {
         EntryVersion version = metadata.version();
         if (version != null) {
            metadataSize = marshall(version).length;
            // We have to write the size of the version
            metadataSize += 4;
         }
         if (metadata.maxIdle() >= 0) {
            metadataSize += 16;
         }
         if (metadata.lifespan() >= 0) {
            metadataSize += 16;
         }
      } else {
         // We have to write the size of the metadata object
         metadataSize += 4;
         metadataSize += marshall(metadata).length;
      }

      // Can we benefit from the smaller payload by adjusting other things and using the old estimate?
      long internalMetadataSize = shouldWriteInternalMetadata(internalMetadata) ?
                                  marshall(internalMetadata).length + 4:
                                  0;
      return estimateSizeOverhead(totalSize + metadataSize + internalMetadataSize);
   }

   @Override
   public long updateMaxIdle(long address, long currentTimeMillis) {
      // 16 bytes for eviction if needed (optional)
      // 8 bytes for linked pointer
      long offset = evictionEnabled ? 24 : 8;

      byte metadataType = MEMORY.getByte(address, offset);

      if ((metadataType & (IMMORTAL + MORTAL)) != 0) {
         return 0;
      }

      // skips over metadataType, hashCode
      offset += 5;

      int keySize = MEMORY.getInt(address, offset);
      offset += 4;

      boolean hasVersion = (metadataType & HAS_VERSION) != 0;
      boolean hasInternalMetadata = requiresInternalMetadataSize(metadataType);

      if ((metadataType & TRANSIENT) != 0) {
         // Skip the metadataSize (if version present), valueSize and the keyBytes
         offset += (hasVersion ? 4 : 0) + (hasInternalMetadata ? 4 : 0 ) + 4 + keySize;
         // Skip the max idle value
         storeLongLittleEndian(address, offset + 8, currentTimeMillis);
         return 0;
      }
      if ((metadataType & TRANSIENT_MORTAL) != 0) {
         // Skip the metadataSize (if version present), valueSize and the keyBytes
         offset += (hasVersion ? 4 : 0) + (hasInternalMetadata ? 4 : 0 ) + 4 + keySize;
         // Skip the lifespan/max idle values and created
         storeLongLittleEndian(address, offset + 24, currentTimeMillis);
         return 0;
      }

      // If we got here it means it is custom type, so we have to read the metadata and update it

      byte[] metadataBytes = new byte[MEMORY.getInt(address, offset)];
      int metadataSize = metadataBytes.length;
      offset += 4;

      int valueSize = MEMORY.getInt(address, offset);
      offset += 4;

      int internalMetadataSize;
      if (hasInternalMetadata) {
         internalMetadataSize = MEMORY.getInt(address, offset);
         offset += 4;
      } else {
         internalMetadataSize = 0;
      }

      // skips over the actual key bytes
      offset += keySize;

      MEMORY.getBytes(address, offset, metadataBytes, 0, metadataSize);

      Metadata metadata = unmarshall(metadataBytes);
      Metadata newMetadata = metadata.builder()
            .maxIdle(currentTimeMillis, TimeUnit.MILLISECONDS)
            .build();

      byte[] newMetadataBytes = marshall(newMetadata, metadataSize);

      int newMetadataSize = newMetadataBytes.length;
      if (newMetadataSize != metadataSize) {
         // The new marshalled size is different then before, we have to rewrite the object!
         // Offset is still set to the end of the key bytes (before metadata)
         long newPointer = MEMORY.allocate(newMetadataSize + offset + valueSize + internalMetadataSize);
         // This writes the next pointer, eviction pointers (if applicable),
         // type, hashCode, keyLength, metadataLength, valueLength and key bytes.
         MEMORY.copy(address, 0, newPointer, 0, offset);
         // This copies the new metadata bytes to the new metadata location
         MEMORY.putBytes(newMetadataBytes, 0, newPointer, offset, newMetadataSize);
         // This copies the value bytes from the old to the new location
         MEMORY.copy(address, offset + metadataSize, newPointer, offset + newMetadataSize, valueSize);
         if (internalMetadataSize > 0) {
            // This copies the internal metadata bytes from the old to the new location
            MEMORY.copy(address, offset + metadataSize + valueSize, newPointer, offset + newMetadataSize + valueSize, internalMetadataSize);
         }

         return newPointer;
      }

      // Replace the metadata bytes with the new ones in place
      MEMORY.putBytes(metadataBytes, 0, address, offset, metadataSize);

      return 0;
   }

   private void storeLongLittleEndian(long destAddres, long offset, long value) {
      MEMORY.putByte(destAddres, offset, (byte) (value >> 56));
      MEMORY.putByte(destAddres, offset + 1, (byte) (value >> 48));
      MEMORY.putByte(destAddres, offset + 2, (byte) (value >> 40));
      MEMORY.putByte(destAddres, offset + 3, (byte) (value >> 32));
      MEMORY.putByte(destAddres, offset + 4, (byte) (value >> 24));
      MEMORY.putByte(destAddres, offset + 5, (byte) (value >> 16));
      MEMORY.putByte(destAddres, offset + 6, (byte) (value >> 8));
      MEMORY.putByte(destAddres, offset + 7, (byte) value);
   }

   private <T> byte[] marshall(T obj) {
      try {
         return marshaller.objectToByteBuffer(obj);
      } catch (IOException e) {
         throw new CacheException(e);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      }
   }

   private <T> byte[] marshall(T obj, int estimatedSize) {
      try {
         return marshaller.objectToByteBuffer(obj, estimatedSize);
      } catch (IOException e) {
         throw new CacheException(e);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      }
   }

   private <T> T unmarshall(byte[] bytes) {
      try {
         //noinspection unchecked
         return (T) marshaller.objectFromByteBuffer(bytes);
      } catch (IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
   }

   private static boolean shouldWriteInternalMetadata(PrivateMetadata metadata) {
      return metadata != null && !metadata.isEmpty();
   }

}
