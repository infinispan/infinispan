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
   private static final UnsafeWrapper UNSAFE = UnsafeWrapper.INSTANCE;

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

   private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

   /**
    * HEADER is composed of hashCode (int), keyLength (int), metadataLength (int), valueLength (int), type (byte)
    */
   private static final int HEADER_LENGTH = 4 + 4 + 4 + 4 + 1;

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
      byte[] metadataBytes;
      if (metadata instanceof EmbeddedMetadata) {

         EntryVersion version = metadata.version();
         byte[] versionBytes;
         if (version != null) {
            type = HAS_VERSION;
            try {
               versionBytes = marshaller.objectToByteBuffer(version);
            } catch (IOException | InterruptedException e) {
               throw new CacheException(e);
            }
         } else {
            type = 0;
            versionBytes = new byte[0];
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
         try {
            metadataBytes = marshaller.objectToByteBuffer(metadata);
         } catch (IOException | InterruptedException e) {
            throw new CacheException(e);
         }
      }
      int keySize = key.getLength();
      int valueSize = value.getLength();
      int metadataSize = metadataBytes.length;
      long totalSize = 8 + HEADER_LENGTH + keySize + metadataSize + valueSize;

      long memoryAddress;
      long memoryOffset;

      // Eviction requires an additional memory pointer at the beginning that points to
      // its linked node
      if (evictionEnabled) {
         memoryAddress = allocator.allocate(totalSize + 8);
         memoryOffset = memoryAddress + 8;
      } else {
         memoryAddress = allocator.allocate(totalSize);
         memoryOffset =  memoryAddress;
      }

      int offset = 0;
      byte[] header = new byte[HEADER_LENGTH];

      Bits.putInt(header, offset, key.hashCode());
      offset += 4;
      Bits.putInt(header, offset, key.getLength());
      offset += 4;
      Bits.putInt(header, offset, metadataBytes.length);
      offset += 4;
      Bits.putInt(header, offset, value.getLength());
      offset += 4;
      header[offset++] = type;


      // Write the empty linked address pointer first
      UNSAFE.putLong(memoryOffset, 0);
      memoryOffset += 8;

      UNSAFE.copyMemory(header, BYTE_ARRAY_BASE_OFFSET, null, memoryOffset, HEADER_LENGTH);
      memoryOffset += HEADER_LENGTH;

      UNSAFE.copyMemory(key.getBytes(), key.backArrayOffset() + BYTE_ARRAY_BASE_OFFSET, null, memoryOffset, keySize);
      memoryOffset += keySize;

      UNSAFE.copyMemory(metadataBytes, BYTE_ARRAY_BASE_OFFSET, null, memoryOffset, metadataSize);
      memoryOffset += metadataSize;

      UNSAFE.copyMemory(value.getBytes(), value.backArrayOffset() + BYTE_ARRAY_BASE_OFFSET, null, memoryOffset,
            valueSize);

      return memoryAddress;
   }

   @Override
   public long determineSize(long address) {
      int beginningOffset = evictionEnabled ? 16 : 8;
      byte[] header = readHeader(beginningOffset + address);

      int keyLength = Bits.getInt(header, 4);
      int metadataLength = Bits.getInt(header, 8);
      int valueLength = Bits.getInt(header, 12);

      return beginningOffset + HEADER_LENGTH + keyLength + metadataLength + valueLength;
   }

   @Override
   public long getNextLinkedPointerAddress(long address) {
      return UNSAFE.getLong(evictionEnabled ? address + 8 : address);
   }

   @Override
   public void updateNextLinkedPointerAddress(long address, long value) {
      UNSAFE.putLong(evictionEnabled ? address + 8 : address, value);
   }

   @Override
   public int getHashCodeForAddress(long address) {
      // 8 bytes for eviction if needed (optional)
      // 8 bytes for linked pointer
      byte[] header = readHeader(evictionEnabled ? address + 16 : address + 8);
      return Bits.getInt(header, 0);
   }

   /**
    * Assumes the address doesn't contain the linked pointer at the beginning
    * @param address the address to read the entry from
    * @return the entry at the memory location
    */
   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> fromMemory(long address) {
      address += (evictionEnabled ? 16 : 8);
      byte[] header = readHeader(address);

      int offset = 0;
      int hashCode = Bits.getInt(header, offset);
      offset += 4;
      byte[] keyBytes = new byte[Bits.getInt(header, offset)];
      offset += 4;
      byte[] metadataBytes = new byte[Bits.getInt(header, offset)];
      offset += 4;
      byte[] valueBytes = new byte[Bits.getInt(header, offset)];
      offset += 4;

      byte metadataType = header[offset++];

      long memoryOffset = address + offset;

      UNSAFE.copyMemory(null, memoryOffset, keyBytes, BYTE_ARRAY_BASE_OFFSET, keyBytes.length);
      memoryOffset += keyBytes.length;
      UNSAFE.copyMemory(null, memoryOffset, metadataBytes, BYTE_ARRAY_BASE_OFFSET, metadataBytes.length);
      memoryOffset += metadataBytes.length;
      UNSAFE.copyMemory(null, memoryOffset, valueBytes, BYTE_ARRAY_BASE_OFFSET, valueBytes.length);
      memoryOffset += valueBytes.length;

      Metadata metadata;
      // This is a custom metadata
      if ((metadataType & 1) == 1) {
         try {
            metadata = (Metadata) marshaller.objectFromByteBuffer(metadataBytes);
         } catch (IOException | ClassNotFoundException e) {
            throw new CacheException(e);
         }
         return internalEntryFactory.create(new WrappedByteArray(keyBytes),
               new WrappedByteArray(valueBytes), metadata);
      } else {
         long lifespan;
         long maxIdle;
         long created;
         long lastUsed;
         offset = 0;
         boolean hasVersion = (metadataType & 2) == 2;
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

   @Override
   public WrappedBytes getKey(long address) {
      address += (evictionEnabled ? 16 : 8);
      byte[] header = readHeader(address);
      int keyLength = Bits.getInt(header, 4);
      byte[] keyBytes = new byte[keyLength];

      UNSAFE.copyMemory(null, address + HEADER_LENGTH, keyBytes, BYTE_ARRAY_BASE_OFFSET, keyBytes.length);
      return new WrappedByteArray(keyBytes);
   }

   private byte[] readHeader(long address) {
      byte[] header = new byte[HEADER_LENGTH];
      UNSAFE.copyMemory(null, address, header, BYTE_ARRAY_BASE_OFFSET, header.length);
      return header;
   }

   /**
    * Assumes the address points to the entry excluding the pointer reference at the beginning
    * @param address the address of an entry to read
    * @param wrappedBytes the key to check if it equals
    * @return whether the key and address are equal
    */
   @Override
   public boolean equalsKey(long address, WrappedBytes wrappedBytes) {
      address += evictionEnabled ? 16 : 8;
      byte[] header = readHeader(address);
      int hashCode = wrappedBytes.hashCode();
      if (hashCode != Bits.getInt(header, 0)) {
         return false;
      }
      int keyLength = Bits.getInt(header, 4);
      byte[] keyBytes = new byte[keyLength];
      UNSAFE.copyMemory(null, address + HEADER_LENGTH, keyBytes,
            BYTE_ARRAY_BASE_OFFSET, keyLength);

      return new WrappedByteArray(keyBytes, hashCode).equalsWrappedBytes(wrappedBytes);
   }
}
