package org.horizon.loader.file;

import org.horizon.io.UnclosableObjectOutputStream;
import org.horizon.loader.BaseCacheStoreTest;
import org.horizon.loader.CacheLoaderException;
import org.horizon.loader.CacheStore;
import org.horizon.loader.StoredEntry;
import org.horizon.loader.bucket.Bucket;
import org.horizon.test.TestingUtil;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

@Test(groups = "unit", testName = "loader.file.FileCacheStoreTest")
public class FileCacheStoreTest extends BaseCacheStoreTest {

   private final String tmpDirectory = TestingUtil.TEST_FILES + File.separator + getClass().getSimpleName();
   private FileCacheStore fcs;

   protected CacheStore createCacheStore() throws CacheLoaderException {
      fcs = new FileCacheStore();
      FileCacheStoreConfig cfg = new FileCacheStoreConfig();
      cfg.setLocation(tmpDirectory);
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      fcs.init(cfg, getCache(), getMarshaller());
      fcs.start();
      return fcs;
   }

   @AfterTest
   @BeforeTest
   public void removeTempDirectory() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
   }

   @Override
   public void testPreload() throws CacheLoaderException {
      super.testPreload();
   }

   @Override
   public void testPurgeExpired() throws Exception {
      long now = System.currentTimeMillis();
      long lifespan = 1000;
      cs.store(new StoredEntry("k1", "v1", now, now + lifespan));
      cs.store(new StoredEntry("k2", "v2", now, now + lifespan));
      cs.store(new StoredEntry("k3", "v3", now, now + lifespan));
      assert cs.containsKey("k1");
      assert cs.containsKey("k2");
      assert cs.containsKey("k3");
      Thread.sleep(lifespan + 100);
      cs.purgeExpired();
      FileCacheStore fcs = (FileCacheStore) cs;
      assert fcs.load("k1") == null;
      assert fcs.load("k2") == null;
      assert fcs.load("k3") == null;
   }

   public void testBucketRemoval() throws Exception {
      Bucket b;
      StoredEntry se = new StoredEntry("test", "value");
      fcs.store(se);
      b = fcs.loadBucketContainingKey("test");
      assert b != null;

      assert !b.getEntries().isEmpty();

      assert new File(fcs.root, b.getBucketName()).exists();

      b.removeEntry("test");
      assert b.getEntries().isEmpty();

      fcs.saveBucket(b);
      assert !new File(fcs.root, b.getBucketName()).exists();
   }

   public void testToStream() throws Exception {
      cs.store(new StoredEntry("k1", "v1", -1, -1));

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(out);
      cs.toStream(new UnclosableObjectOutputStream(oos));
      oos.flush();
      oos.close();
      out.close();

      ObjectInputStream ois = null;
      try {
         ois = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()));
         assert ois.readInt() == 1 : "we have 3 different buckets";
         assert ois.readObject().equals("k1".hashCode() + "");
         assert ois.readInt() > 0; //size on disk
      } finally {
         if (ois != null) ois.close();
      }
   }
}
