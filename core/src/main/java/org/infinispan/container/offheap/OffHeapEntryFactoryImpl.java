package org.infinispan.container.offheap;

import java.io.IOException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.factories.annotations.Inject;
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

   private Marshaller marshaller;
   private OffHeapMemoryAllocator allocator;
   private TimeService timeService;
   private InternalEntryFactory internalEntryFactory;
   private boolean evictionEnabled;

   // If custom than we just store the metadata as is (no other bits should be used)
   private static final byte CUSTOM = 1;
   // Version can be set with any combination of the following types
   private static final byte HAS_VERSION = 2;
   // Only one of the following should ever be set
   private static final byte IMMORTAL = 1 << 2;
   private static final byte MORTAL = 1 << 3;
   private static final byte TRANSIENT = 1 << 4;
   private static final byte TRANSIENT_MORTAL = 1 << 5;

   /**
    * HEADER is composed of type (byte), hashCode (int), keyLength (int), valueLength (int)
    * Note that metadata is not included as this is now optional
    */
   private static final int HEADER_LENGTH = 1 + 4 + 4 + 4;

   @Inject
   public void inject(Marshaller marshaller, OffHeapMemoryAllocator allocator, TimeService timeService,
         InternalEntryFactory internalEntryFactory, Configuration configuration) {
      this.marshaller = marshaller;
      this.allocator = allocator;
      this.timeService = timeService;
      this.internalEntryFactory = internalEntryFactory;
      this.evictionEnabled = configuration.memory().isEvictionEnabled();
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
      byte type;
      boolean shouldWriteMetadataSize = false;
      byte[] metadataBytes;
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
            versionBytes = EMPTY_BYTES;
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
            Bits.putLong(metadataBytes, 8, timeService.wallClockTime());
            System.arraycopy(versionBytes, 0, metadataBytes, 16, versionBytes.length);
         } else if (lifespan < 0 && maxIdle > -1) {
            type |= TRANSIENT;
            metadataBytes = new byte[16 + versionBytes.length];
            Bits.putLong(metadataBytes, 0, maxIdle);
            Bits.putLong(metadataBytes, 8, timeService.wallClockTime());
            System.arraycopy(versionBytes, 0, metadataBytes, 16, versionBytes.length);
         } else {
            type |= TRANSIENT_MORTAL;
            metadataBytes = new byte[32 + versionBytes.length];
            Bits.putLong(metadataBytes, 0, maxIdle);
            Bits.putLong(metadataBytes, 8, lifespan);
            long time = timeService.wallClockTime();
            Bits.putLong(metadataBytes, 16, time);
            Bits.putLong(metadataBytes, 24, time);
            System.arraycopy(versionBytes, 0, metadataBytes, 32, versionBytes.length);
         }
      } else {
         type = CUSTOM;
         shouldWriteMetadataSize = true;
         try {
            metadataBytes = marshaller.objectToByteBuffer(metadata);
         } catch (IOException | InterruptedException e) {
            throw new CacheException(e);
         }
      }
      int keySize = key.getLength();
      int metadataSize = metadataBytes.length;
      int valueSize = value.getLength();

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
      MEMORY.putInt(memoryAddress, offset, value.getLength());
      offset += 4;

      MEMORY.putBytes(key.getBytes(), key.backArrayOffset(), memoryAddress, offset, keySize);
      offset += keySize;

      MEMORY.putBytes(metadataBytes, 0, memoryAddress, offset, metadataSize);
      offset += metadataSize;

      MEMORY.putBytes(value.getBytes(), value.backArrayOffset(), memoryAddress, offset, valueSize);
      offset += valueSize;

      assert offset == totalSize;

      return memoryAddress;
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
         metadataLength += 4;
      } else {
         metadataLength = 0;
      }

      int valueLength = MEMORY.getInt(entryAddress, headerOffset);
      headerOffset += 4;

      return headerOffset + keyLength + metadataLength + valueLength;
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
         case IMMORTAL:
            metadataBytes = EMPTY_BYTES;
            break;
         case MORTAL:
            metadataBytes = new byte[16];
            break;
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

      byte[] valueBytes = new byte[MEMORY.getInt(address, offset)];
      offset += 4;

      MEMORY.getBytes(address, offset, keyBytes, 0, keyBytes.length);
      offset += keyBytes.length;
      MEMORY.getBytes(address, offset, metadataBytes, 0, metadataBytes.length);
      offset += metadataBytes.length;
      MEMORY.getBytes(address, offset, valueBytes, 0, valueBytes.length);
      offset += valueBytes.length;

      Metadata metadata;
      // This is a custom metadata
      if ((metadataType & CUSTOM) == CUSTOM) {
         try {
            metadata = (Metadata) marshaller.objectFromByteBuffer(metadataBytes);
         } catch (IOException | ClassNotFoundException e) {
            throw new CacheException(e);
         }
         return internalEntryFactory.create(new WrappedByteArray(keyBytes, hashCode),
               new WrappedByteArray(valueBytes), metadata);
      } else {
         long lifespan;
         long maxIdle;
         long created;
         long lastUsed;
         offset = 0;
         boolean hasVersion = (metadataType & HAS_VERSION) == HAS_VERSION;
         // Ignore CUSTOM and VERSION to find type
         switch (metadataType & 0xFC) {
            case IMMORTAL:
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
            case TRANSIENT_MORTAL:
               lifespan = Bits.getLong(metadataBytes, offset);
               maxIdle = Bits.getLong(metadataBytes, offset += 8);
               created = Bits.getLong(metadataBytes, offset += 8);
               lastUsed = Bits.getLong(metadataBytes, offset += 8);
               break;
            default:
               throw new IllegalArgumentException("Unsupported type: " + metadataType);
         }
         if (hasVersion) {
            try {
               EntryVersion version = (EntryVersion) marshaller.objectFromByteBuffer(metadataBytes, offset,
                     metadataBytes.length - offset);
               return internalEntryFactory.create(new WrappedByteArray(keyBytes, hashCode),
                     new WrappedByteArray(valueBytes), version, created, lifespan, lastUsed, maxIdle);
            } catch (IOException | ClassNotFoundException e) {
               throw new CacheException(e);
            }
         } else {
            return internalEntryFactory.create(new WrappedByteArray(keyBytes, hashCode),
                  new WrappedByteArray(valueBytes), (Metadata) null, created, lifespan, lastUsed, maxIdle);
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

   static private boolean requiresMetadataSize(byte type) {
      return (type & (CUSTOM | HAS_VERSION)) != 0;
   }
}
