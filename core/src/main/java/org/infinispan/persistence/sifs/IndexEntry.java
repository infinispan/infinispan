package org.infinispan.persistence.sifs;

import java.io.IOException;

import org.infinispan.util.SoftBPlusTree;

class IndexEntry extends EntryInfo {
   private static final Log log = Log.getLog(IndexEntry.class);

   volatile EntryRecord record;

   IndexEntry(int file, int offset, int numRecords) {
      super(file, offset, numRecords);
   }

   private EntryRecord ensureRecord(FileProvider dataFileProvider) throws IOException {
      EntryRecord record = this.record;
      if (record != null) {
         return record;
      }
      try (FileProvider.Handle handle = dataFileProvider.getFile(file)) {
         if (handle == null) {
            throw new SoftBPlusTree.IndexNodeOutdatedException(file + ":" + offset);
         }
         int readOffset = offset < 0 ? ~offset : offset;
         EntryHeader header = EntryRecord.readEntryHeader(handle, readOffset);
         if (header == null) {
            log.tracef("Could not read header from %d:%d", file, readOffset);
            return null;
         }
         byte[] key = EntryRecord.readKey(handle, header, readOffset);
         record = new EntryRecord(header, key);
         this.record = record;
         log.tracef("Loaded header and key from %d:%d", file, readOffset);
         return record;
      }
   }

   byte[] loadKey(FileProvider dataFileProvider) throws IOException {
      EntryRecord record = ensureRecord(dataFileProvider);
      return record != null ? record.getKey() : null;
   }

   EntryHeader getHeader(FileProvider dataFileProvider) throws IOException {
      EntryRecord record = ensureRecord(dataFileProvider);
      return record != null ? record.getHeader() : null;
   }

   EntryRecord loadRecord(FileProvider dataFileProvider, long wallClockTime,
         boolean loadValues, boolean saveValue) throws IOException {
      EntryRecord record = ensureRecord(dataFileProvider);
      if (record == null) return null;
      EntryHeader header = record.getHeader();
      if (header.valueLength() <= 0) {
         log.tracef("Entry %s:%d matched, it is a tombstone.", file, offset);
         return null;
      }
      if (wallClockTime >= 0 && header.expiryTime() > 0 && header.expiryTime() <= wallClockTime) {
         log.tracef("Key on %s:%d matched but expired.", file, offset);
         return null;
      }
      log.tracef("Loaded from %s:%d", file, offset);
      if (record.getValue() != null) {
         return record;
      }
      int readOffset = offset < 0 ? ~offset : offset;
      try (FileProvider.Handle handle = dataFileProvider.getFile(file)) {
         if (handle == null) {
            throw new SoftBPlusTree.IndexNodeOutdatedException(file + ":" + offset);
         }
         if (loadValues) {
            return record.loadMetadataAndValue(handle, readOffset, saveValue);
         } else {
            return record.loadMetadata(handle, readOffset);
         }
      }
   }
}
