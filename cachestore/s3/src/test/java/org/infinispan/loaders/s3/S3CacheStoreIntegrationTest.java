package org.infinispan.loaders.s3;

import static org.testng.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.io.UnclosableObjectInputStream;
import org.infinispan.io.UnclosableObjectOutputStream;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.s3.jclouds.JCloudsBucket;
import org.infinispan.loaders.s3.jclouds.JCloudsConnection;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Adrian Cole
 * @version $Id$
 * @since 4.0
 */
@Test(groups = "unit", sequential = true, testName = "loaders.s3.S3CacheStoreIntegrationTest")
public class S3CacheStoreIntegrationTest extends BaseCacheStoreTest {

   private String proxyHost;
   private int proxyPort = -1;
   private int maxConnections = 20;
   private boolean isSecure = false;
   private Class<? extends S3Connection> connectionClass;
   private Class<? extends S3Bucket> bucketClass;
   private String csBucket;
   private String cs2Bucket;
   private String accessKey;
   private String secretKey;

   private static final String sysAWSAccessKeyId = System
         .getProperty("jclouds.aws.accesskeyid");
   private static final String sysAWSSecretAccessKey = System
         .getProperty("jclouds.aws.secretaccesskey");

   @BeforeTest
   @Parameters({"jclouds.aws.accesskeyid", "jclouds.aws.secretaccesskey"})
   protected void setUpClient(@Optional String AWSAccessKeyId,
                              @Optional String AWSSecretAccessKey) throws Exception {

      accessKey = (AWSAccessKeyId == null) ? sysAWSAccessKeyId : AWSAccessKeyId;
      secretKey = (AWSSecretAccessKey == null) ? sysAWSSecretAccessKey : AWSSecretAccessKey;

      if (accessKey == null || accessKey.trim().equals("") ||
            secretKey == null || secretKey.trim().equals("")) {
         accessKey = "dummy";
         secretKey = "dummy";
         connectionClass = MockS3Connection.class;
         bucketClass = MockS3Bucket.class;
      } else {
         connectionClass = JCloudsConnection.class;
         bucketClass = JCloudsBucket.class;
         proxyHost = "localhost";  // TODO  not yet used
         proxyPort = 8888; // TODO  not yet used
      }
      csBucket = (System.getProperty("user.name")
            + "." + this.getClass().getSimpleName()).toLowerCase();
      System.err.printf("accessKey: %1s, connectionClass: %2s, bucketClass: %3s, bucket: %4s%n", accessKey,
                        connectionClass, bucketClass, csBucket);

      cs2Bucket = csBucket + "2";
   }

   protected CacheStore createCacheStore() throws Exception {
      return createAndStartCacheStore(csBucket);
   }

   protected CacheStore createAnotherCacheStore() throws Exception {
      return createAndStartCacheStore(cs2Bucket);
   }

   private CacheStore createAndStartCacheStore(String bucket) throws Exception {
      S3CacheStore cs = new S3CacheStore();
      S3CacheStoreConfig cfg = new S3CacheStoreConfig();
      cfg.setBucket(bucket);
      cfg.setAwsAccessKey(accessKey);
      cfg.setAwsSecretKey(secretKey);
      cfg.setProxyHost(proxyHost);
      cfg.setProxyPort(proxyPort);
      cfg.setSecure(isSecure);
      cfg.setMaxConnections(maxConnections);
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      cs.init(cfg, getCache(), getMarshaller(), connectionClass.newInstance(), bucketClass.newInstance());
      cs.start();
      return cs;
   }

   /*  Changes below are needed to support testing of multiple cache stores */

   protected CacheStore cs2;

   @BeforeMethod
   @Override
   public void setUp() throws Exception {
      super.setUp();
      cs.clear();
      Set entries = cs.loadAll();
      assert entries.size() == 0;
      cs2 = createAnotherCacheStore();
      cs2.clear();
      entries = cs2.loadAll();
      assert entries.size() == 0;
   }


   @AfterMethod
   @Override
   public void tearDown() throws CacheLoaderException {
      if (cs != null) {
         cs.clear();
         cs.stop();

      }
      cs = null;
      if (cs2 != null) {
         cs2.clear();

         cs2.stop();
      }
      cs2 = null;
   }


   @SuppressWarnings("unchecked")
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
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()));
      cs2.fromStream(new UnclosableObjectInputStream(ois));

      Set<InternalCacheEntry> set = cs2.loadAll();

      assertEquals(set.size(), 3);
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }


   @SuppressWarnings("unchecked")
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

      // first pop the start bytes
      byte[] dummy = new byte[8];
      ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
      int bytesRead = in.read(dummy, 0, 8);
      assertEquals(bytesRead, 8);
      for (int i = 1; i < 9; i++) assert dummy[i - 1] == i : "Start byte stream corrupted!";
      cs2.fromStream(new UnclosableObjectInputStream(new ObjectInputStream(in)));
      bytesRead = in.read(dummy, 0, 8);
      assertEquals(bytesRead, 8);
      for (int i = 8; i > 0; i--) assert dummy[8 - i] == i : "Start byte stream corrupted!";

      Set<InternalCacheEntry> set = cs2.loadAll();

      assertEquals(set.size(), 3);
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }
}