package org.infinispan.loader.s3;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.io.UnclosableObjectInputStream;
import org.infinispan.io.UnclosableObjectOutputStream;
import org.infinispan.loader.BaseCacheStoreTest;
import org.infinispan.loader.CacheLoaderException;
import org.infinispan.loader.CacheStore;
import org.jets3t.service.model.S3Bucket;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Adrian Cole
 * @version $Id$
 * @since 4.0
 */
@Test(groups = "unit", testName = "loader.s3.S3CacheStoreIntegrationTest")
public class S3CacheStoreIntegrationTest extends BaseCacheStoreTest {
   private String csBucket;
   private String cs2Bucket;
   private String accessKey;
   private String secretKey;
   private S3Connection s3Connection;

   @BeforeTest(enabled = false)
   public void initRealConnection() {
      csBucket = "your-favorite-bucket-doesnt-need-to-exist-but-if-it-does-this-test-will-nuke-it";
      cs2Bucket = csBucket + "2";
      accessKey = "youraccesskey";
      secretKey = "yoursecretkey";
      s3Connection = new Jets3tS3Connection();
   }

   @BeforeTest(enabled = true)
   public void initMockConnection() {
      csBucket = "horizontesting";
      cs2Bucket = csBucket + "2";
      accessKey = "dummyaccess";
      secretKey = "dummysecret";
      s3Connection = new MockS3Connection();
   }

   @AfterTest
   public void removeS3Buckets() throws Exception {
      s3Connection.removeBucketIfEmpty(new S3Bucket(csBucket));
      s3Connection.removeBucketIfEmpty(new S3Bucket(cs2Bucket));
      s3Connection = null;
   }

   protected CacheStore createCacheStore() throws CacheLoaderException {
      return createAndStartCacheStore(csBucket);
   }

   protected CacheStore createAnotherCacheStore() throws CacheLoaderException {
      return createAndStartCacheStore(cs2Bucket);
   }

   private CacheStore createAndStartCacheStore(String bucket) throws CacheLoaderException {
      S3CacheStore cs = new S3CacheStore();
      S3CacheStoreConfig cfg = new S3CacheStoreConfig();
      cfg.setBucket(bucket);
      cfg.setAwsAccessKey(accessKey);
      cfg.setAwsSecretKey(secretKey);
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      cs.init(cfg, getCache(), getMarshaller(), s3Connection);
      cs.start();
      return cs;
   }

   /*  Changes below are needed to support testing of multiple cache stores */

   protected CacheStore cs2;

   @BeforeMethod
   @Override
   public void setUp() throws Exception {
      super.setUp();
      cs2 = createAnotherCacheStore();
   }


   @AfterMethod
   @Override
   public void tearDown() throws CacheLoaderException {
      super.tearDown();
      if (cs2 != null) {
         cs2.clear();
         cs2.stop();
      }
      cs2 = null;
   }


   @Override
   public void testStreamingAPI() throws IOException, ClassNotFoundException, CacheLoaderException {
      cs.store(InternalEntryFactory.create("k1", "v1", -1, -1));
      cs.store(InternalEntryFactory.create("k2", "v2", -1, -1));
      cs.store(InternalEntryFactory.create("k3", "v3", -1, -1));

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(out);
      cs.toStream(new UnclosableObjectOutputStream(oos));
      oos.flush();
      oos.close();
      out.close();
      cs2.clear();
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()));
      cs2.fromStream(new UnclosableObjectInputStream(ois));

      Set<InternalCacheEntry> set = cs2.loadAll();

      assert set.size() == 3;
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }

   @Override
   public void testStreamingAPIReusingStreams() throws IOException, ClassNotFoundException, CacheLoaderException {
      cs.store(InternalEntryFactory.create("k1", "v1", -1, -1));
      cs.store(InternalEntryFactory.create("k2", "v2", -1, -1));
      cs.store(InternalEntryFactory.create("k3", "v3", -1, -1));

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] dummyStartBytes = {1, 2, 3, 4, 5, 6, 7, 8};
      byte[] dummyEndBytes = {8, 7, 6, 5, 4, 3, 2, 1};
      out.write(dummyStartBytes);
      ObjectOutputStream oos = new ObjectOutputStream(out);
      cs.toStream(new UnclosableObjectOutputStream(oos));
      oos.flush();
      oos.close();
      out.write(dummyEndBytes);
      out.close();
      cs2.clear();

      // first pop the start bytes
      byte[] dummy = new byte[8];
      ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
      int bytesRead = in.read(dummy, 0, 8);
      assert bytesRead == 8;
      for (int i = 1; i < 9; i++) assert dummy[i - 1] == i : "Start byte stream corrupted!";
      cs2.fromStream(new UnclosableObjectInputStream(new ObjectInputStream(in)));
      bytesRead = in.read(dummy, 0, 8);
      assert bytesRead == 8;
      for (int i = 8; i > 0; i--) assert dummy[8 - i] == i : "Start byte stream corrupted!";

      Set<InternalCacheEntry> set = cs2.loadAll();

      assert set.size() == 3;
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }
}