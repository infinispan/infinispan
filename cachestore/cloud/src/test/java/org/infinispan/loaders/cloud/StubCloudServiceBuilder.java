package org.infinispan.loaders.cloud;

import org.infinispan.CacheDelegate;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.marshall.Marshaller;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.integration.StubBlobStoreContextBuilder;

public class StubCloudServiceBuilder {
   public static CloudCacheStore buildCloudCacheStoreWithStubCloudService(String bucket, Marshaller marshaller) throws CacheLoaderException {
      CloudCacheStore cs = new CloudCacheStore();
      CloudCacheStoreConfig cfg = new CloudCacheStoreConfig();
      cfg.setBucketPrefix(bucket);
      cfg.setCloudService("unit-test-stub");
      cfg.setIdentity("unit-test-stub");
      cfg.setPassword("unit-test-stub");
      cfg.setProxyHost("unit-test-stub");
      cfg.setProxyPort("unit-test-stub");
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      StubBlobStoreContextBuilder cb = new StubBlobStoreContextBuilder();
      BlobStoreContext ctx = cb.buildBlobStoreContext();
      cs.init(cfg, new CacheDelegate("aName"), marshaller, ctx, ctx.getBlobStore(), ctx.getAsyncBlobStore(), false);
      return cs;
   }
}
