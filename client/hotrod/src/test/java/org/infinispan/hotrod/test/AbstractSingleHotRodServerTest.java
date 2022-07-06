package org.infinispan.hotrod.test;

import static org.infinispan.hotrod.HotRodServerExtension.builder;

import org.infinispan.api.Infinispan;
import org.infinispan.hotrod.HotRodServerExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractSingleHotRodServerTest<C> {

   protected C cache;
   protected Infinispan container;
   protected String cacheName;

   @RegisterExtension
   static HotRodServerExtension server = builder()
         .build();

   @BeforeEach
   public void setup() {
      container = container();
      cacheName = server.cacheName();
      cache = cache();
   }

   @AfterEach
   public void internalTeardown() {
      assert container != null : "Container is null";
      teardown();
      container.close();
      container = null;
      cache = null;
   }

   protected abstract void teardown();

   abstract Infinispan container();

   abstract C cache();
}
