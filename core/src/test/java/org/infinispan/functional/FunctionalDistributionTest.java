package org.infinispan.functional;

import java.util.Collections;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.test.TestingUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Krzysztof Sobolewski &lt;Krzysztof.Sobolewski@atende.pl&gt;
 */
@Test(groups = "functional", testName = "functional.FunctionalDistributionTest")
public class FunctionalDistributionTest extends AbstractFunctionalTest {
   public FunctionalDistributionTest() {
      numNodes = 4;
      // we want some non-owners and some secondary owners:
      numDistOwners = 2;
      /*
       * The potential problem arises in async mode: the non-primary owner
       * executes, then forwards to the primary owner, which executes and
       * forwards to all non-primary owners, including the originator, which
       * executes again. In sync mode this does not cause problems because the
       * second invocation on the originator happens before the first is
       * committed so it receives the same input data. Not so in async mode.
       */
      isSync = false;
   }

   @BeforeClass
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
   }

   public void testDistributionFromPrimaryOwner() throws InterruptedException {
      Object key = "testDistributionFromPrimaryOwner";
      doTestDistribution(key, cacheManagers.stream()
            .map(cm -> cm.<Object, Integer>getCache(DIST).getAdvancedCache())
            .filter(cache -> cache.getDistributionManager().getPrimaryLocation(key)
                  .equals(cache.getRpcManager().getAddress()))
            .findAny()
            .get());
   }

   public void testDistributionFromSecondaryOwner() throws InterruptedException {
      Object key = "testDistributionFromSecondaryOwner";
      doTestDistribution(key, cacheManagers.stream()
            .map(cm -> cm.<Object, Integer>getCache(DIST).getAdvancedCache())
            // owner...
            .filter(cache -> cache.getDistributionManager().getLocality(key).isLocal()
                  // ...but not primary owner
                  && !cache.getDistributionManager().getPrimaryLocation(key)
                        .equals(cache.getRpcManager().getAddress()))
            .findAny()
            .get());
   }

   public void testDistributionFromNonOwner() throws InterruptedException {
      Object key = "testDistributionFromNonOwner";
      doTestDistribution(key, cacheManagers.stream()
            .map(cm -> cm.<Object, Integer>getCache(DIST).getAdvancedCache())
            .filter(cache -> !cache.getDistributionManager().getLocality(key).isLocal())
            .findAny()
            .get());
   }

   private void doTestDistribution(Object key, AdvancedCache<Object, Integer> originator) throws InterruptedException {
      ReadWriteMap<Object, Integer> rw = ReadWriteMapImpl.create(FunctionalMapImpl.create(originator));

      // with empty cache:
      iterate(key, rw, 1);
      // again:
      iterate(key, rw, 2);
   }

   private void iterate(Object key, ReadWriteMap<Object, Integer> rw, int expectedValue) throws InterruptedException {
      rw.eval(key, entry -> {
               // we need a small delay so that the value gets committed before the replication finishes:
               TestingUtil.sleepThread(10);
               return entry.set(entry.find().orElse(0) + 1);
            }).join();

      // since we're in async mode, we need to wait before the replication finishes
      Thread.sleep(100); // TODO: find a better way?

      // we want to ensure that each of the owners executes the function only once:
      Assert.assertEquals(
            (Object) cacheManagers.stream()
                  .map(cm -> cm.<Object, Integer>getCache(DIST).getAdvancedCache())
                  .filter(cache -> cache.getDistributionManager().getLocality(key).isLocal())
                  .map(cache -> cache.getDataContainer().get(key).getValue())
                  .collect(Collectors.toList()),
            Collections.nCopies(numDistOwners, expectedValue));
   }
}
