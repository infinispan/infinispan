package org.infinispan.functional;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
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

   public void testDistributionFromPrimaryOwner() throws Exception {
      Object key = "testDistributionFromPrimaryOwner";
      doTestDistribution(key, cacheManagers.stream()
            .map(cm -> cm.<Object, Integer>getCache(DIST).getAdvancedCache())
            .filter(cache -> cache.getDistributionManager().getCacheTopology().getDistribution(key).isPrimary())
            .findAny()
            .get());
   }

   public void testDistributionFromSecondaryOwner() throws Exception {
      Object key = "testDistributionFromSecondaryOwner";
      doTestDistribution(key, cacheManagers.stream()
            .map(cm -> cm.<Object, Integer>getCache(DIST).getAdvancedCache())
            // owner...
            .filter(cache -> cache.getDistributionManager().getCacheTopology().getDistribution(key).isWriteBackup())
            .findAny()
            .get());
   }

   public void testDistributionFromNonOwner() throws Exception {
      Object key = "testDistributionFromNonOwner";
      doTestDistribution(key, cacheManagers.stream()
            .map(cm -> cm.<Object, Integer>getCache(DIST).getAdvancedCache())
            .filter(cache -> !cache.getDistributionManager().getCacheTopology().isWriteOwner(key))
            .findAny()
            .get());
   }

   private void doTestDistribution(Object key, AdvancedCache<Object, Integer> originator) throws Exception {
      ReadWriteMap<Object, Integer> rw = ReadWriteMapImpl.create(FunctionalMapImpl.create(originator));

      // with empty cache:
      iterate(key, rw, 1);
      // again:
      iterate(key, rw, 2);
   }

   private void iterate(Object key, ReadWriteMap<Object, Integer> rw, int expectedValue) throws Exception {
      List<AdvancedCache<Object, Object>> owners = cacheManagers
            .stream().map(cm -> cm.getCache(DIST).getAdvancedCache())
            .filter(cache -> cache.getDistributionManager().getCacheTopology().isWriteOwner(key))
            .collect(Collectors.toList());

      CyclicBarrier barrier = new CyclicBarrier(numDistOwners + 1);
      for (AdvancedCache cache : owners) {
         BlockingInterceptor bi = new BlockingInterceptor<>(barrier, ReadWriteKeyCommand.class, true, false);
         cache.getAsyncInterceptorChain().addInterceptorBefore(bi, EntryWrappingInterceptor.class);
      }

      // While the command execution could be async the BlockingInterceptor would block us on primary owner == originator
      // On backup == originator the blocking interceptor is not hit until
      Future<Void> f = fork(() -> rw.eval(key, entry -> entry.set(entry.find().orElse(0) + 1)).join());

      barrier.await(10, TimeUnit.SECONDS);
      for (AdvancedCache cache : owners) {
         cache.getAsyncInterceptorChain().findInterceptorWithClass(BlockingInterceptor.class).suspend(true);
      }
      barrier.await(10, TimeUnit.SECONDS);

      for (AdvancedCache cache : owners) {
         cache.getAsyncInterceptorChain().removeInterceptor(BlockingInterceptor.class);
      }

      f.get(10, TimeUnit.SECONDS);

      // we want to ensure that each of the owners executes the function only once:
      Assert.assertEquals(owners.stream()
                  .map(cache -> cache.getDataContainer().get(key).getValue())
                  .collect(Collectors.toList()),
            Collections.nCopies(numDistOwners, expectedValue));
   }
}
