/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.query.tx;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;

import static org.infinispan.query.helper.TestQueryHelperFactory.*;
import static org.infinispan.test.TestingUtil.withTx;

@Test(groups = "functional", testName = "query.tx.TransactionalQueryTest")
public class TransactionalQueryTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
            .enable()
            .indexLocalOnly(false)
            .addProperty("hibernate.search.default.directory_provider", "ram")
            .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @BeforeMethod
   public void initialize() throws Exception {
      // Initialize the cache
      withTx(tm(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            for (int i = 0; i < 100; i++) {
               cache.put(String.valueOf(i), new Session(String.valueOf(i)));
            }
            return null;
         }
      });
   }

   public void run() throws Exception {
      // Verify querying works
      createCacheQuery(cache, "", "Id:2?");

      // Remove something that exists
      withTx(tm(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.remove("50");
            return null;
         }
      });

      // Remove something that doesn't exist with a transaction
      // This also fails without using a transaction
      withTx(tm(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.remove("200");
            return null;
         }
      });
   }

   @Indexed(index = "SessionIndex")
   public static class Session {
      private String m_id;

      public Session(String id) {
         m_id = id;
      }

      @Field(name = "Id")
      public String getId() {
         return m_id;
      }
   }
}
