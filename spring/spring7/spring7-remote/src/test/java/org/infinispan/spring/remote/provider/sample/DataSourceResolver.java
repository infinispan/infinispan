package org.infinispan.spring.remote.provider.sample;

import javax.sql.XADataSource;

import org.h2.jdbcx.JdbcDataSource;

import com.arjuna.ats.internal.jdbc.DynamicClass;

/**
 * Required by JBoss Transactions for DataSource resolving.
 *
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
public class DataSourceResolver implements DynamicClass {

   @Override
   public XADataSource getDataSource(String url) {
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL(String.format("jdbc:%s;DB_CLOSE_DELAY=-1", url));
      dataSource.setUser("sa");
      dataSource.setPassword("");
      return dataSource;
   }
}
