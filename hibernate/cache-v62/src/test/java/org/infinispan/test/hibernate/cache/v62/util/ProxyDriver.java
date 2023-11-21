package org.infinispan.test.hibernate.cache.v62.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.h2.Driver;

/**
 * @since 15.0
 **/
public class ProxyDriver extends Driver {
   static AtomicInteger connections = new AtomicInteger(0);
   @Override
   public Connection connect(String url, Properties info) throws SQLException {
      System.out.printf("%d %s%n", connections.incrementAndGet(), url);
      return new ProxyConnection(connections.get(), super.connect(url, info));
   }
}
