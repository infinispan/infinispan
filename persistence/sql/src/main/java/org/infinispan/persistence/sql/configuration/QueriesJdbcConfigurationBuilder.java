package org.infinispan.persistence.sql.configuration;

import static org.infinispan.persistence.sql.configuration.QueriesJdbcConfiguration.DELETE;
import static org.infinispan.persistence.sql.configuration.QueriesJdbcConfiguration.DELETE_ALL;
import static org.infinispan.persistence.sql.configuration.QueriesJdbcConfiguration.SELECT;
import static org.infinispan.persistence.sql.configuration.QueriesJdbcConfiguration.SELECT_ALL;
import static org.infinispan.persistence.sql.configuration.QueriesJdbcConfiguration.SIZE;
import static org.infinispan.persistence.sql.configuration.QueriesJdbcConfiguration.UPSERT;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationChildBuilder;

/**
 * QueriesJdbcConfigurationBuilder.
 *
 * @author William Burns
 * @since 13.0
 */
public class QueriesJdbcConfigurationBuilder<S extends AbstractJdbcStoreConfigurationBuilder<?, S>> extends AbstractJdbcStoreConfigurationChildBuilder<S> implements Builder<QueriesJdbcConfiguration> {

   private final AttributeSet attributes;

   public QueriesJdbcConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, S> builder) {
      super(builder);
      this.attributes = QueriesJdbcConfiguration.attributeDefinitionSet();
   }

   /**
    * Configures the select statement to be used when reading entries from the database. Note all parameters must be
    * named (i.e. <b>:myname</b>) and the parameters must be the same name and order as the one provided to {@link
    * #delete(String)}.
    *
    * @param select the select statement to use
    * @return this
    */
   public QueriesJdbcConfigurationBuilder<S> select(String select) {
      attributes.attribute(SELECT).set(select);
      return this;
   }

   /**
    * Configures the select all statement to be used when reading all entries from the database. No parameters may be
    * used.
    *
    * @param selectAll the select all statement to use
    * @return this
    */
   public QueriesJdbcConfigurationBuilder<S> selectAll(String selectAll) {
      attributes.attribute(SELECT_ALL).set(selectAll);
      return this;
   }

   /**
    * Configures the delete statement to be used when removing entries from the database. Note all parameters must be
    * named (i.e. <b>:myname</b>) and the parameters must be the same name and order as the one provided to {@link
    * #select(String)}.
    *
    * @param delete the delete statement to use
    * @return this
    */
   public QueriesJdbcConfigurationBuilder<S> delete(String delete) {
      attributes.attribute(DELETE).set(delete);
      return this;
   }

   /**
    * Configures the delete all statement to be used when clearing the store. No parameters may be used.
    *
    * @param deleteAll the delete all statement to use
    * @return this
    */
   public QueriesJdbcConfigurationBuilder<S> deleteAll(String deleteAll) {
      attributes.attribute(DELETE_ALL).set(deleteAll);
      return this;
   }

   /**
    * Configures the upsert statement to be used when writing entries to the database. Note all parameters must be named
    * (i.e. <b>:myname</b>).
    *
    * @param upsert the upsert statement to use
    * @return this
    */
   public QueriesJdbcConfigurationBuilder<S> upsert(String upsert) {
      attributes.attribute(UPSERT).set(upsert);
      return this;
   }

   /**
    * Configures the size statement to be used when determining the size of the store. No parameters may be used.
    *
    * @param size the size statement to use
    * @return this
    */
   public QueriesJdbcConfigurationBuilder<S> size(String size) {
      attributes.attribute(SIZE).set(size);
      return this;
   }

   @Override
   public void validate() {
      if (attributes.attribute(SIZE).isNull() || attributes.attribute(SELECT).isNull() ||
            attributes.attribute(SELECT_ALL).isNull()) {
         throw org.infinispan.persistence.jdbc.common.logging.Log.CONFIG.requiredStatementsForQueryStoreLoader();
      }
   }

   public void validate(boolean isLoader) {
      validate();
      if (!isLoader && (attributes.attribute(DELETE).isNull() || attributes.attribute(DELETE_ALL).isNull() ||
            attributes.attribute(UPSERT).isNull())) {
         throw org.infinispan.persistence.jdbc.common.logging.Log.CONFIG.requiredStatementsForQueryStoreWriter();
      }
   }

   @Override
   public QueriesJdbcConfiguration create() {
      return new QueriesJdbcConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(QueriesJdbcConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
