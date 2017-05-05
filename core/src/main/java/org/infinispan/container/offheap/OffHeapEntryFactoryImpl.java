package org.infinispan.container.offheap;

import java.io.IOException;

import org.infinispan.commands.InvocationRecord;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.StreamingMarshaller;
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
   private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

   private StreamingMarshaller marshaller;
   private OffHeapMemoryAllocator allocator;
   private TimeService timeService;
   private InternalEntryFactory internalEntryFactory;
   private boolean evictionEnabled;

   // If custom than we just store the metadata as is (no other bits should be used)
   private static final int CUSTOM = 1;
   // Version can be set with any combination of the following types
   private static final int HAS_VERSION = 2;
   // Only one of the following should ever be set
   private static final int MORTAL = 1 << 2;
   private static final int TRANSIENT = 1 << 3;
   private static final int TOMBSTONE = 1 << 4;
   private static final int HAS_INVOCATIONS = 1 << 5;
   private static final int EMBEDDED_TYPE_MASK = TRANSIENT | MORTAL;

   private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

   /**
    * HEADER is composed of hashCode (int), keyLength (int), metadataLength (int), valueLength (int), type (byte)
    */
   private static final int HEADER_LENGTH = 4 + 4 + 4 + 4 + 1;

   @Inject
   public void inject(StreamingMarshaller marshaller, OffHeapMemoryAllocator allocator, TimeService timeService,
         InternalEntryFactory internalEntryFactory, Configuration configuration) {
      this.marshaller = marshaller;
      this.allocator = allocator;
      this.timeService = timeService;
      this.internalEntryFactory = internalEntryFactory;
      this.evictionEnabled = configuration.memory().size() > 0;
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
         int versionInvocationsLength = 0;
         EntryVersion version = metadata.version();
         byte[] versionBytes;
         if (version != null) {
            type = HAS_VERSION;
            try {
               versionBytes = marshaller.objectToByteBuffer(version);
               versionInvocationsLength += versionBytes.length;
            } catch (IOException | InterruptedException e) {
               throw new CacheException(e);
            }
         } else {
            type = 0;
            versionBytes = EMPTY_BYTE_ARRAY;
         }
         ByteBuffer invocationBytes;
         if (metadata.lastInvocation() != null) {
            type |= HAS_INVOCATIONS;
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
               metadataBytes = invocationBytes == null ? EMPTY_BYTE_ARRAY : invocationBytes.toBytes();
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
            Bits.putLong(metadataBytes, 16, timeService.wallClockTime());
            Bits.putLong(metadataBytes, 24, timeService.wallClockTime());
            putVersionsInvocations(metadataBytes, 32, type, versionBytes, invocationBytes);
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
      int valueSize = value == null ? 0 : value.getLength();
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

      if (value == null) {
         type |= TOMBSTONE;
      }

      int offset = 0;
      byte[] header = new byte[HEADER_LENGTH];

      Bits.putInt(header, offset, key.hashCode());
      offset += 4;
      Bits.putInt(header, offset, key.getLength());
      offset += 4;
      Bits.putInt(header, offset, metadataBytes.length);
      offset += 4;
      Bits.putInt(header, offset, valueSize);
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

      if (value != null) {
         UNSAFE.copyMemory(value.getBytes(), value.backArrayOffset() + BYTE_ARRAY_BASE_OFFSET, null, memoryOffset,
               valueSize);
      }

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

      byte type = header[offset++];

      long memoryOffset = address + offset;

      UNSAFE.copyMemory(null, memoryOffset, keyBytes, BYTE_ARRAY_BASE_OFFSET, keyBytes.length);
      memoryOffset += keyBytes.length;
      UNSAFE.copyMemory(null, memoryOffset, metadataBytes, BYTE_ARRAY_BASE_OFFSET, metadataBytes.length);
      memoryOffset += metadataBytes.length;

      WrappedByteArray value = null;
      if ((type & TOMBSTONE) == 0) {
         UNSAFE.copyMemory(null, memoryOffset, valueBytes, BYTE_ARRAY_BASE_OFFSET, valueBytes.length);
         value = new WrappedByteArray(valueBytes);
      } else {
         assert valueBytes.length == 0;
      }

      Metadata metadata;
      if ((type & CUSTOM) == CUSTOM) {
         try {
            metadata = (Metadata) marshaller.objectFromByteBuffer(metadataBytes);
         } catch (IOException | ClassNotFoundException e) {
            throw new CacheException(e);
         }
         return internalEntryFactory.create(new WrappedByteArray(keyBytes),
               value, metadata);
      } else {
         long lifespan;
         long maxIdle;
         long created;
         long lastUsed;
         offset = 0;
         boolean hasVersion = (type & HAS_VERSION) == HAS_VERSION;
         boolean hasInvocations = (type & HAS_INVOCATIONS) == HAS_INVOCATIONS;
         // Ignore CUSTOM and VERSION to find type
         switch (type & EMBEDDED_TYPE_MASK) {
            case 0:
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
            case (TRANSIENT | MORTAL):
               lifespan = Bits.getLong(metadataBytes, offset++);
               maxIdle = Bits.getLong(metadataBytes, offset += 8);
               created = Bits.getLong(metadataBytes, offset += 8);
               lastUsed = Bits.getLong(metadataBytes, offset += 8);
               break;
            default:
               throw new IllegalArgumentException("Unsupported type: " + type);
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
            throw new CacheException(e);
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
