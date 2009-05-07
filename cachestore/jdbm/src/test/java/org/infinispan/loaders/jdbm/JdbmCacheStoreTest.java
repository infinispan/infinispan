package org.infinispan.loaders.jdbm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.io.UnclosableObjectOutputStream;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.jdbm.JdbmCacheStoreTest")
public class JdbmCacheStoreTest extends BaseCacheStoreTest {

   private JdbmCacheStore fcs;
   private String tmpDirectory;

   @BeforeTest
   @Parameters({"basedir"})
   protected void setUpTempDir() {
      tmpDirectory = "." + TestingUtil.TEST_PATH + File.separator + getClass().getSimpleName();
   }

   @AfterTest
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   @Override
   protected CacheStore createCacheStore() throws CacheLoaderException {
      clearTempDir();
      fcs = new JdbmCacheStore();
      JdbmCacheStoreConfig cfg = new JdbmCacheStoreConfig();
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
      InternalCacheEntry k1 = InternalEntryFactory.create("k1", "v1", lifespan);
      InternalCacheEntry k2 = InternalEntryFactory.create("k2", "v2", lifespan);
      InternalCacheEntry k3 = InternalEntryFactory.create("k3", "v3", lifespan);
      cs.store(k1);
      cs.store(k2);
      cs.store(k3);
      assert cs.containsKey("k1");
      assert cs.containsKey("k2");
      assert cs.containsKey("k3");
      Thread.sleep(lifespan + 100);
      cs.purgeExpired();
      JdbmCacheStore fcs = (JdbmCacheStore) cs;
      assert fcs.load("k1") == null;
      assert fcs.load("k2") == null;
      assert fcs.load("k3") == null;
   }

   public void testIterator() throws Exception {
      InternalCacheEntry k1 = InternalEntryFactory.create("k1", "v1");
      InternalCacheEntry k2 = InternalEntryFactory.create("k2", "v2");
      cs.store(k1);
      cs.store(k2);
      
      Set<InternalCacheEntry> set = cs.loadAll();
      Iterator<InternalCacheEntry> i = set.iterator();
      assert i.hasNext() == true;
      assert i.hasNext() == true;
      assert i.next().getKey().equals("k1");
      assert i.next().getKey().equals("k2");
      assert i.hasNext() == false;
      assert i.hasNext() == false;
      try {
         i.next();
         assert false;
      } catch (NoSuchElementException e) {}
   }

   public void testToStream() throws Exception {
      cs.store(InternalEntryFactory.create("k1", "v1", -1, -1));

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(out);
      cs.toStream(new UnclosableObjectOutputStream(oos));
      oos.flush();
      oos.close();
      out.close();

      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()));
      try {
         Object readObject = ois.readObject();
         assert readObject instanceof InternalCacheEntry;
         readObject = ois.readObject();
         assert readObject == null;
         assert ois.available() == 0;
      } finally {
         ois.close();
      }
   }
}
