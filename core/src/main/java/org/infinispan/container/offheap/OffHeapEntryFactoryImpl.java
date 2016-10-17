package org.infinispan.container.offheap;

import java.io.IOException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.TimeService;

import sun.misc.Unsafe;

/**
 * Factory that can create CacheEntry instances from off-heap memory.
 * @author wburns
 * @since 9.0
 */
public class OffHeapEntryFactoryImpl implements OffHeapEntryFactory {
   private static final Unsafe UNSAFE = UnsafeHolder.UNSAFE;

   private Marshaller marshaller;
   private OffHeapMemoryAllocator allocator;
   private TimeService timeService;
   private InternalEntryFactory internalEntryFactory;

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

   private static final int HEADER_LENGTH = 4 + 4 + 4 + 4 + 1;

   @Inject
   public void inject(Marshaller marshaller, OffHeapMemoryAllocator allocator, TimeService timeService,
         InternalEntryFactory internalEntryFactory) {
      this.marshaller = marshaller;
      this.allocator = allocator;
      this.timeService = timeService;
      this.internalEntryFactory = internalEntryFactory;
   }

   /**
    * Create an entry off-heap.  The first 8 bytes will always be 0, reserved for a future reference to another entry
    * @param key
    * @param value
    * @param metadata
    * @return
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
            System.arraycopy(metadataBytes, 0, versionBytes, 16, versionBytes.length);
         } else if (lifespan < 0 && maxIdle > -1) {
            type |= TRANSIENT;
            metadataBytes = new byte[16 + versionBytes.length];
            Bits.putLong(metadataBytes, 0, maxIdle);
            Bits.putLong(metadataBytes, 8, timeService.wallClockTime());
            System.arraycopy(metadataBytes, 0, versionBytes, 16, versionBytes.length);
         } else {
            type |= TRANSIENT_MORTAL;
            metadataBytes = new byte[32 + versionBytes.length];
            Bits.putLong(metadataBytes, 0, maxIdle);
            Bits.putLong(metadataBytes, 8, lifespan);
            Bits.putLong(metadataBytes, 16, timeService.wallClockTime());
            Bits.putLong(metadataBytes, 24, timeService.wallClockTime());
            System.arraycopy(metadataBytes, 0, versionBytes, 16, versionBytes.length);
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

      long memoryAddress = allocator.allocate(totalSize);

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

      long memoryOffset = memoryAddress;

      // Write the empty linked address pointer first
      UNSAFE.putLong(memoryAddress, 0);
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
      long offset = 8 + HEADER_LENGTH + address;
      return 8 + HEADER_LENGTH + UNSAFE.getInt(offset) + UNSAFE.getInt(offset + 8) + UNSAFE.getInt(offset + 8);
   }

   /**
    * Assumes the addres doesn't contain the linked pointer at the beginning
    * @param address
    * @return
    */
   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> fromMemory(long address) {
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
               lifespan = Bits.getLong(metadataBytes, offset++);
               created = Bits.getLong(metadataBytes, offset += 8);
               lastUsed = -1;
               break;
            case TRANSIENT:
               lifespan = -1;
               maxIdle = Bits.getLong(metadataBytes, offset++);
               created = -1;
               lastUsed = Bits.getLong(metadataBytes, offset += 8);
               break;
            case TRANSIENT_MORTAL:
               lifespan = Bits.getLong(metadataBytes, offset++);
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

   private byte[] readHeader(long address) {
      byte[] header = new byte[HEADER_LENGTH];
      UNSAFE.copyMemory(null, address, header, BYTE_ARRAY_BASE_OFFSET, header.length);
      return header;
   }

   /**
    * Assumes the address points to the entry excluding the pointer reference at the beginning
    * @param address
    * @param wrappedBytes
    * @return
    */
   @Override
   public boolean equalsKey(long address, WrappedBytes wrappedBytes) {
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
