package org.infinispan.loader.bdbje;

import org.easymock.EasyMock;
import org.infinispan.loader.BaseCacheStoreTest;
import org.infinispan.loader.CacheLoaderException;
import org.infinispan.loader.CacheStore;
import org.infinispan.loader.modifications.Clear;
import org.infinispan.loader.modifications.Modification;
import org.infinispan.loader.modifications.Remove;
import org.infinispan.loader.modifications.Store;
import org.infinispan.test.TestingUtil;
import org.infinispan.container.entries.InternalEntryFactory;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Adrian Cole
 * @version $Id: $
 * @since 4.0
 */
@Test(groups = "unit", enabled = true, testName = "loader.bdbje.BdbjeCacheStoreIntegrationTest")
public class BdbjeCacheStoreIntegrationTest extends BaseCacheStoreTest {

   protected CacheStore createCacheStore() throws CacheLoaderException {
      CacheStore cs = new BdbjeCacheStore();
      String tmpDir = TestingUtil.TEST_FILES;
      String tmpCLLoc = tmpDir + "/Horizon-BdbjeCacheStoreIntegrationTest";
      TestingUtil.recursiveFileRemove(tmpCLLoc);

      BdbjeCacheStoreConfig cfg = new BdbjeCacheStoreConfig();
      cfg.setLocation(tmpCLLoc);
      cfg.setPurgeSynchronously(true);
      cs.init(cfg, getCache(), getMarshaller());
      cs.start();
      return cs;
   }

   /**
    * this is the same as the superclass, except that it doesn't attempt read-committed
    */
   @Override
   public void testTwoPhaseCommit() throws CacheLoaderException {
      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(InternalEntryFactory.create("k1", "v1")));
      mods.add(new Store(InternalEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      Transaction tx = EasyMock.createNiceMock(Transaction.class);
      cs.prepare(mods, tx, false);
      cs.commit(tx);

      assert cs.load("k2").getValue().equals("v2");
      assert !cs.containsKey("k1");

      cs.clear();

      mods = new ArrayList<Modification>();
      mods.add(new Store(InternalEntryFactory.create("k1", "v1")));
      mods.add(new Store(InternalEntryFactory.create("k2", "v2")));
      mods.add(new Clear());
      mods.add(new Store(InternalEntryFactory.create("k3", "v3")));

      cs.prepare(mods, tx, false);
      cs.commit(tx);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert cs.containsKey("k3");
   }

   /**
    * this is the same as the superclass, except that it doesn't attempt read-committed
    */
   @Override
   public void testRollback() throws CacheLoaderException {

      cs.store(InternalEntryFactory.create("old", "old"));

      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(InternalEntryFactory.create("k1", "v1")));
      mods.add(new Store(InternalEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      mods.add(new Remove("old"));
      Transaction tx = EasyMock.createNiceMock(Transaction.class);
      cs.prepare(mods, tx, false);
      cs.rollback(tx);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert cs.containsKey("old");

      mods = new ArrayList<Modification>();
      mods.add(new Store(InternalEntryFactory.create("k1", "v1")));
      mods.add(new Store(InternalEntryFactory.create("k2", "v2")));
      mods.add(new Clear());
      mods.add(new Store(InternalEntryFactory.create("k3", "v3")));

      cs.prepare(mods, tx, false);
      cs.rollback(tx);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert !cs.containsKey("k3");
      assert cs.containsKey("old");
   }

}
