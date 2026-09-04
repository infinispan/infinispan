package org.infinispan.commons.configuration.attributes;

import org.infinispan.commons.configuration.io.ConfigurationSchemaVersion;

final class TestConfigurationSchemaVersion implements ConfigurationSchemaVersion {
   private final int major;
   private final int minor;

   TestConfigurationSchemaVersion(int major, int minor) {
      this.major = major;
      this.minor = minor;
   }

   @Override
   public String getURI() {
      return "urn:infinispan:test:" + major + "." + minor;
   }

   @Override
   public int getMajor() {
      return major;
   }

   @Override
   public int getMinor() {
      return minor;
   }

   @Override
   public boolean since(int otherMajor, int otherMinor) {
      return major > otherMajor || (major == otherMajor && minor >= otherMinor);
   }
}
