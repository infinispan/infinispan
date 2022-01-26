package org.infinispan.container.versioning;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.infinispan.notifications.cachemanagerlistener.event.impl.EventImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Test numeric version generator logic
 */
@Test(groups = "functional", testName = "container.versioning.NumericVersionGeneratorTest")
public class NumericVersionGeneratorTest {

   public void testGenerateVersion() {
      RankCalculator rankCalculator = new RankCalculator();
      Configuration config = new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).build();
      NumericVersionGenerator vg = new NumericVersionGenerator();
      TestingUtil.inject(vg, rankCalculator, config);
      vg.start();
      vg.resetCounter();

      TestAddress addr1 = new TestAddress(1);
      TestAddress addr2 = new TestAddress(2);
      TestAddress addr3 = new TestAddress(1);
      List<Address> members = Arrays.asList(addr1, addr2, addr3);
      rankCalculator.updateRank(new EventImpl(null, null, Event.Type.VIEW_CHANGED, members, members, addr2, 1));

      assertEquals(0x1000200000000L, rankCalculator.getVersionPrefix());
      assertEquals(new NumericVersion(0x1000200000001L), vg.generateNew());
      assertEquals(new NumericVersion(0x1000200000002L), vg.generateNew());

      members = Arrays.asList(addr2, addr3);
      rankCalculator.updateRank(new EventImpl(null, null, Event.Type.VIEW_CHANGED, members, members, addr2, 2));

      assertEquals(0x2000100000000L, rankCalculator.getVersionPrefix());
      assertEquals(new NumericVersion(0x2000100000003L), vg.generateNew());
   }

   class TestAddress implements Address {
      int addressNum;

      TestAddress(int addressNum) {
         this.addressNum = addressNum;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         if (!super.equals(o)) return false;

         TestAddress that = (TestAddress) o;

         if (addressNum != that.addressNum) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = super.hashCode();
         result = 31 * result + addressNum;
         return result;
      }

      @Override
      public int compareTo(Address o) {
         TestAddress oa = (TestAddress) o;
         return addressNum - oa.addressNum;
      }
   }

}
