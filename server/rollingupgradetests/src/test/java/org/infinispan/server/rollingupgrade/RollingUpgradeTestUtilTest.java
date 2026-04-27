package org.infinispan.server.rollingupgrade;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class RollingUpgradeTestUtilTest {

   @ParameterizedTest
   @CsvSource({
         "image://quay.io/infinispan/server:16.0.4, 16.0.4",
         "image://quay.io/infinispan/server:16.0.4@sha256:abcdef1234567890, 16.0.4",
         "image://localhost:5000/infinispan/server:16.0.4, 16.0.4",
         "image://localhost:5000/infinispan/server:16.0.4@sha256:abcdef1234567890, 16.0.4",
         "image://quay.io/infinispan/server:latest, latest",
         "image://quay.io/infinispan/server:latest@sha256:abcdef1234567890, latest",
   })
   void testExtractVersionFromImageLabel(String label, String expectedVersion) {
      assertThat(RollingUpgradeTestUtil.extractVersionFromImageLabel(label)).isEqualTo(expectedVersion);
   }

   @ParameterizedTest
   @CsvSource({
         "image://quay.io/infinispan/server",
         "image://quay.io/infinispan/server@sha256:abcdef1234567890",
         "latest",
         "16.0.4",
         "file:///path/to/server",
   })
   void testExtractVersionFromImageLabelReturnsNull(String label) {
      assertThat(RollingUpgradeTestUtil.extractVersionFromImageLabel(label)).isNull();
   }

   @Test
   void testExtractVersionFromNullLabel() {
      assertThat(RollingUpgradeTestUtil.extractVersionFromImageLabel(null)).isNull();
   }
}
