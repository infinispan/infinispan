package org.infinispan.loaders.file;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.io.UnclosableObjectOutputStream;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

@Test(groups = "unit", testName = "loaders.file.FileCacheStoreTest")
public class FileCacheStoreTest extends BaseCacheStoreTest {

   private FileCacheStore fcs;
   private String tmpDirectory;

   @BeforeTest
   @Parameters({"basedir"})
   protected void setUpTempDir(String basedir) {
      tmpDirectory = basedir + TestingUtil.TEST_PATH + File.separator + getClass().getSimpleName();
   }

   @AfterTest
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   protected CacheStore createCacheStore() throws CacheLoaderException {
      clearTempDir();
      fcs = new FileCacheStore();
      FileCacheStoreConfig cfg = new FileCacheStoreConfig();
      cfg.setLocation(tmpDirectory);
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      fcs.init(cfg, getCache(), getMarshaller());
      fcs.start();
      return fcs;
   }

   @Override
   public void testPreload() throws CacheLoaderException {
      super.testPreload();
   }

   @Override
   public void testPurgeExpired() throws Exception {
      long lifespan = 1000;
      cs.store(InternalEntryFactory.create("k1", "v1", lifespan));
      cs.store(InternalEntryFactory.create("k2", "v2", lifespan));
      cs.store(InternalEntryFactory.create("k3", "v3", lifespan));
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
      InternalCacheEntry se = InternalEntryFactory.create("test", "value");
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
      cs.store(InternalEntryFactory.create("k1", "v1", -1, -1));

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
