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

   DbMetaData(DatabaseType type, Integer majorVersion, Integer minorVersion) {
      Objects.requireNonNull(type);
      this.type = type;
      this.majorVersion = majorVersion == null ? -1 : majorVersion;
      this.minorVersion = minorVersion == null ? -1 : minorVersion;
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
}