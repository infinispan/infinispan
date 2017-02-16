package org.infinispan.persistence.jdbc.table.management;

import java.util.Objects;

import org.infinispan.persistence.jdbc.DatabaseType;

/**
 * @author Ryan Emerson
 */
public class DbMetaData {
   private final DatabaseType type;
   private final int majorVersion;
   private final int minorVersion;
   private final boolean upsertDisabled;
   private final boolean indexingDisabled;

   public DbMetaData(DatabaseType type, Integer majorVersion, Integer minorVersion, boolean upsertDisabled, boolean indexingDisabled) {
      Objects.requireNonNull(type);
      this.type = type;
      this.majorVersion = majorVersion == null ? -1 : majorVersion;
      this.minorVersion = minorVersion == null ? -1 : minorVersion;
      this.upsertDisabled = upsertDisabled;
      this.indexingDisabled = indexingDisabled;
   }

   DatabaseType getType() {
      return type;
   }

   int getMajorVersion() {
      return majorVersion;
   }

   int getMinorVersion() {
      return minorVersion;
   }

   boolean isUpsertDisabled() {
      return upsertDisabled;
   }

   boolean isIndexingDisabled() {
      return indexingDisabled;
   }
}
