package org.infinispan.manager.impl;

import static org.infinispan.manager.impl.ClusterMemberVersions.UNKNOWN_VERSION;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "manager.impl.ClusterMemberVersionsTest")
public class ClusterMemberVersionsTest extends AbstractInfinispanTest {

   public void testSingleVersionIsNotARollingUpgrade() {
      assertFalse(ClusterMemberVersions.isRollingUpgrade(List.of()));
      assertFalse(ClusterMemberVersions.isRollingUpgrade(List.of("16.0.0.Final")));
      assertFalse(ClusterMemberVersions.isRollingUpgrade(List.of("16.0.0.Final", "16.0.0.Final", "16.0.0.Final")));
   }

   public void testDifferentVersionsAreARollingUpgrade() {
      assertTrue(ClusterMemberVersions.isRollingUpgrade(List.of("16.0.0.Final", "16.1.0.Final")));
      assertTrue(ClusterMemberVersions.isRollingUpgrade(List.of("16.0.0.Final", "16.0.0.Final", "16.1.0.Final")));
   }

   public void testUnknownVersionsAreIgnored() {
      assertFalse(ClusterMemberVersions.isRollingUpgrade(List.of(UNKNOWN_VERSION)));
      assertFalse(ClusterMemberVersions.isRollingUpgrade(List.of(UNKNOWN_VERSION, UNKNOWN_VERSION)));
      // a member which did not answer must not be mistaken for a member running a different version
      assertFalse(ClusterMemberVersions.isRollingUpgrade(List.of(UNKNOWN_VERSION, "16.0.0.Final")));
      assertFalse(ClusterMemberVersions.isRollingUpgrade(List.of("16.0.0.Final", UNKNOWN_VERSION, "16.0.0.Final")));
      assertTrue(ClusterMemberVersions.isRollingUpgrade(List.of(UNKNOWN_VERSION, "16.0.0.Final", "16.1.0.Final")));
   }
}
