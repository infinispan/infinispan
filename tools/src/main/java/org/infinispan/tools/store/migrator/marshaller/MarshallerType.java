package org.infinispan.tools.store.migrator.marshaller;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@Deprecated
public enum MarshallerType {
   LEGACY(8),
   CURRENT(9),
   CUSTOM(-1);

   final int majorVersion;

   MarshallerType(int majorVersion) {
      this.majorVersion = majorVersion;
   }

   public int getMajorVersion() {
      return majorVersion;
   }
}
