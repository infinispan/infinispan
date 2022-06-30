package org.infinispan.persistence.jdbc.impl.table;

import java.util.Objects;

import org.infinispan.persistence.jdbc.common.DatabaseType;

/**
 * @author Ryan Emerson
 */
public class DbMetaData {
   private final DatabaseType type;
   private final int majorVersion;
   private final int minorVersion;
   private final int maxTableNameLength;
   private final boolean upsertDisabled;
   private final boolean indexingDisabled;
   private final boolean segmentedDisabled;

   public DbMetaData(DatabaseType type, Integer majorVersion, Integer minorVersion, int maxTableNameLength, boolean upsertDisabled,
         boolean indexingDisabled, boolean segmentedDisabled) {
      this.type = Objects.requireNonNull(type);
      this.majorVersion = majorVersion == null ? -1 : majorVersion;
      this.minorVersion = minorVersion == null ? -1 : minorVersion;
      this.maxTableNameLength = maxTableNameLength;
      this.upsertDisabled = upsertDisabled;
      this.indexingDisabled = indexingDisabled;
      this.segmentedDisabled = segmentedDisabled;
   }

   public DatabaseType getType() {
      return type;
   }

   public int getMajorVersion() {
      return majorVersion;
   }

   public int getMinorVersion() {
      return minorVersion;
   }

   public boolean isUpsertDisabled() {
      return upsertDisabled;
   }

   public boolean isIndexingDisabled() {
      return indexingDisabled;
   }

   public boolean isSegmentedDisabled() {
      return segmentedDisabled;
   }

   public int getMaxTableNameLength() {
      return maxTableNameLength;
   }
}
