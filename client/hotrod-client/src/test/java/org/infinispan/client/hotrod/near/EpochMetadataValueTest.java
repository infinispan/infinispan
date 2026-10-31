package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link EpochMetadataValue}: the delegation contract and the null-delegate guard.
 */
@Test(groups = "unit", testName = "client.hotrod.near.EpochMetadataValueTest")
public class EpochMetadataValueTest {

   private MetadataValue<String> newDelegate(String value) {
      return new MetadataValueImpl<>(1L, 60, 2L, 30, 3L, value);
   }

   public void testDelegationWithNonNullDelegate() {
      MetadataValue<String> delegate = newDelegate("hello");
      EpochMetadataValue<String> emv = new EpochMetadataValue<>(delegate, 7, 2);

      assertThat(emv.getEpoch()).isEqualTo(7);
      assertThat(emv.getSegment()).isEqualTo(2);
      assertThat(emv.getDelegate()).isSameAs(delegate);

      assertThat(emv.getValue()).isEqualTo("hello");
      assertThat(emv.getCreated()).isEqualTo(delegate.getCreated());
      assertThat(emv.getLifespan()).isEqualTo(delegate.getLifespan());
      assertThat(emv.getLastUsed()).isEqualTo(delegate.getLastUsed());
      assertThat(emv.getMaxIdle()).isEqualTo(delegate.getMaxIdle());
      assertThat(emv.getVersion()).isEqualTo(delegate.getVersion());
   }

   public void testNullDelegateThrows() {
      // delegate must be non-null — verify the contract is enforced at construction time.
      assertThatNullPointerException()
            .isThrownBy(() -> new EpochMetadataValue<>(null, 3, 0))
            .withMessage("delegate");
   }

   public void testToString() {
      MetadataValue<String> delegate = newDelegate("world");
      EpochMetadataValue<String> emv = new EpochMetadataValue<>(delegate, 5, 1);

      String s = emv.toString();
      assertThat(s).contains("epoch=5").contains("delegate=");
   }
}
