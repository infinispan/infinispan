package org.horizon.loader.bdbje;

import org.easymock.EasyMock;
import org.horizon.loader.BaseCacheStoreTest;
import org.horizon.loader.CacheLoaderException;
import org.horizon.loader.CacheStore;
import org.horizon.loader.StoredEntry;
import org.horizon.loader.modifications.Clear;
import org.horizon.loader.modifications.Modification;
import org.horizon.loader.modifications.Remove;
import org.horizon.loader.modifications.Store;
import org.horizon.test.TestingUtil;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Adrian Cole
 * @version $Id: $
 * @since 1.0
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
      mods.add(new Store(new StoredEntry("k1", "v1", -1, -1)));
      mods.add(new Store(new StoredEntry("k2", "v2", -1, -1)));
      mods.add(new Remove("k1"));
      Transaction tx = EasyMock.createNiceMock(Transaction.class);
      cs.prepare(mods, tx, false);
      cs.commit(tx);

      assert cs.load("k2").getValue().equals("v2");
      assert !cs.containsKey("k1");

      cs.clear();

      mods = new ArrayList<Modification>();
      mods.add(new Store(new StoredEntry("k1", "v1", -1, -1)));
      mods.add(new Store(new StoredEntry("k2", "v2", -1, -1)));
      mods.add(new Clear());
      mods.add(new Store(new StoredEntry("k3", "v3", -1, -1)));

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

      cs.store(new StoredEntry("old", "old", -1, -1));

      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(new StoredEntry("k1", "v1", -1, -1)));
      mods.add(new Store(new StoredEntry("k2", "v2", -1, -1)));
      mods.add(new Remove("k1"));
      mods.add(new Remove("old"));
      Transaction tx = EasyMock.createNiceMock(Transaction.class);
      cs.prepare(mods, tx, false);
      cs.rollback(tx);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert cs.containsKey("old");

      mods = new ArrayList<Modification>();
      mods.add(new Store(new StoredEntry("k1", "v1", -1, -1)));
      mods.add(new Store(new StoredEntry("k2", "v2", -1, -1)));
      mods.add(new Clear());
      mods.add(new Store(new StoredEntry("k3", "v3", -1, -1)));

      cs.prepare(mods, tx, false);
      cs.rollback(tx);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert !cs.containsKey("k3");
      assert cs.containsKey("old");
   }

}
