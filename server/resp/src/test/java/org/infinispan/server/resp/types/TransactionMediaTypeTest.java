package org.infinispan.server.resp.types;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.resp.TransactionOperationsTest;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.resp.types.TransactionMediaTypeTest")
public class TransactionMediaTypeTest extends TransactionOperationsTest {

   private boolean simpleCache;
   private MediaType valueType;

   @Override
   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {
      if (simpleCache) {
         configurationBuilder.clustering().cacheMode(CacheMode.LOCAL).simpleCache(true);
      } else {
         configurationBuilder.clustering().cacheMode(cacheMode);
         configurationBuilder.invocationBatching().enable(true);
      }
      configurationBuilder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      configurationBuilder.encoding().value().mediaType(valueType.toString());
   }

   private TransactionMediaTypeTest withValueType(MediaType type) {
      this.valueType = type;
      return this;
   }

   private TransactionMediaTypeTest withSimpleCache() {
      this.simpleCache = true;
      return this;
   }

   private TransactionMediaTypeTest withCacheMode(CacheMode mode) {
      this.cacheMode = mode;
      return this;
   }

   @Override
   public Object[] factory() {
      List<TransactionMediaTypeTest> instances = new ArrayList<>();
      MediaType[] types = new MediaType[] {
            MediaType.APPLICATION_OCTET_STREAM,
            MediaType.APPLICATION_PROTOSTREAM,
            MediaType.APPLICATION_OBJECT,
            MediaType.TEXT_PLAIN,
      };
      for (MediaType value : types) {
         instances.add(new TransactionMediaTypeTest().withValueType(value).withCacheMode(CacheMode.LOCAL));
         instances.add(new TransactionMediaTypeTest().withValueType(value).withSimpleCache());
         instances.add(new TransactionMediaTypeTest().withValueType(value).withCacheMode(CacheMode.LOCAL).withAuthorization());
         instances.add(new TransactionMediaTypeTest().withValueType(value).withSimpleCache().withAuthorization());
      }
      return instances.toArray();
   }

   @Override
   protected String parameters() {
      return "[simpleCache=" + simpleCache + ", cacheMode=" + cacheMode + ", value=" + valueType + "]";
   }
}
