package org.infinispan.persistence.jdbc.table.management;

import org.infinispan.persistence.jdbc.DatabaseType;

import java.util.Objects;

/**
 * @author Ryan Emerson
 */
class DbMetaData {
   private final DatabaseType type;
   private final int majorVersion;
   private final int minorVersion;
   private final boolean upsertDisabled;

   DbMetaData(DatabaseType type, Integer majorVersion, Integer minorVersion, boolean upsertDisabled) {
      Objects.requireNonNull(type);
      this.type = type;
      this.majorVersion = majorVersion == null ? -1 : majorVersion;
      this.minorVersion = minorVersion == null ? -1 : minorVersion;
      this.upsertDisabled = upsertDisabled;
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
}