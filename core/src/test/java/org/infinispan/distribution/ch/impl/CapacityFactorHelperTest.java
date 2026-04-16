package org.infinispan.distribution.ch.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "distribution.ch.impl.CapacityFactorHelperTest")
public class CapacityFactorHelperTest {

   public void testNullListIsDefault() {
      assertThat(CapacityFactorHelper.isDefaultCapacityFactor((List<Float>) null)).isTrue();
   }

   public void testEmptyListIsDefault() {
      assertThat(CapacityFactorHelper.isDefaultCapacityFactor(List.of())).isTrue();
   }

   public void testAllOnesListIsDefault() {
      assertThat(CapacityFactorHelper.isDefaultCapacityFactor(List.of(1.0f, 1.0f, 1.0f))).isTrue();
   }

   public void testMixedListIsNotDefault() {
      assertThat(CapacityFactorHelper.isDefaultCapacityFactor(List.of(1.0f, 0.5f))).isFalse();
   }

   public void testAllZeroListIsNotDefault() {
      assertThat(CapacityFactorHelper.isDefaultCapacityFactor(List.of(0f, 0f))).isFalse();
   }

   public void testNullMapIsDefault() {
      assertThat(CapacityFactorHelper.isDefaultCapacityFactor((Map<?, Float>) null)).isTrue();
   }

   public void testEmptyMapIsDefault() {
      assertThat(CapacityFactorHelper.isDefaultCapacityFactor(Collections.<String, Float>emptyMap())).isTrue();
   }

   public void testAllOnesMapIsDefault() {
      Map<String, Float> map = new HashMap<>();
      map.put("A", 1.0f);
      map.put("B", 1.0f);
      assertThat(CapacityFactorHelper.isDefaultCapacityFactor(map)).isTrue();
   }

   public void testMixedMapIsNotDefault() {
      Map<String, Float> map = new HashMap<>();
      map.put("A", 1.0f);
      map.put("B", 0.5f);
      assertThat(CapacityFactorHelper.isDefaultCapacityFactor(map)).isFalse();
   }

   public void testBothNullListsAreEqual() {
      assertThat(CapacityFactorHelper.isCapacityFactorsEquals((List<Float>) null, null)).isTrue();
   }

   public void testNullEqualsAllOnesListDefaults() {
      assertThat(CapacityFactorHelper.isCapacityFactorsEquals(null, List.of(1.0f, 1.0f))).isTrue();
      assertThat(CapacityFactorHelper.isCapacityFactorsEquals(List.of(1.0f), null)).isTrue();
   }

   public void testNullNotEqualToNonDefaultList() {
      assertThat(CapacityFactorHelper.isCapacityFactorsEquals(null, List.of(0.5f))).isFalse();
      assertThat(CapacityFactorHelper.isCapacityFactorsEquals(List.of(0.5f), null)).isFalse();
   }

   public void testSameNonDefaultListsAreEqual() {
      List<Float> a = List.of(0.5f, 1.0f);
      List<Float> b = List.of(0.5f, 1.0f);
      assertThat(CapacityFactorHelper.isCapacityFactorsEquals(a, b)).isTrue();
   }

   public void testDifferentNonDefaultListsAreNotEqual() {
      assertThat(CapacityFactorHelper.isCapacityFactorsEquals(List.of(0.5f), List.of(0.3f))).isFalse();
   }

   public void testBothNullMapsAreEqual() {
      assertThat(CapacityFactorHelper.isCapacityFactorsEquals((Map<?, Float>) null, null)).isTrue();
   }

   public void testNullEqualsAllOnesMapDefaults() {
      Map<String, Float> defaults = new HashMap<>();
      defaults.put("A", 1.0f);
      assertThat(CapacityFactorHelper.isCapacityFactorsEquals(null, defaults)).isTrue();
      assertThat(CapacityFactorHelper.isCapacityFactorsEquals(defaults, null)).isTrue();
   }

   public void testNullNotEqualToNonDefaultMap() {
      Map<String, Float> nonDefault = new HashMap<>();
      nonDefault.put("A", 0.5f);
      assertThat(CapacityFactorHelper.isCapacityFactorsEquals(null, nonDefault)).isFalse();
      assertThat(CapacityFactorHelper.isCapacityFactorsEquals(nonDefault, null)).isFalse();
   }

   public void testNullListHashCodeEqualsAllOnesHashCode() {
      assertThat(CapacityFactorHelper.capacityFactorHashCode((List<Float>) null))
            .isEqualTo(CapacityFactorHelper.capacityFactorHashCode(List.of(1.0f, 1.0f)));
   }

   public void testNullMapHashCodeEqualsAllOnesHashCode() {
      Map<String, Float> defaults = new HashMap<>();
      defaults.put("A", 1.0f);
      assertThat(CapacityFactorHelper.capacityFactorHashCode((Map<?, Float>) null))
            .isEqualTo(CapacityFactorHelper.capacityFactorHashCode(defaults));
   }

   public void testNonDefaultHashCodeDiffers() {
      assertThat(CapacityFactorHelper.capacityFactorHashCode(List.of(0.5f)))
            .isNotEqualTo(CapacityFactorHelper.capacityFactorHashCode((List<Float>) null));
   }
}
