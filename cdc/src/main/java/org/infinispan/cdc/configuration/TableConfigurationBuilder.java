package org.infinispan.cdc.configuration;

import java.util.ArrayList;
import java.util.Collection;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationChildBuilder;

public class TableConfigurationBuilder
      extends AbstractJdbcStoreConfigurationChildBuilder<ChangeDataCaptureConfigurationBuilder>
      implements Builder<TableConfiguration> {

   private final AttributeSet attributes;
   private final ColumnConfigurationBuilder pkb;
   private final Collection<ColumnConfigurationBuilder> ccbList;

   protected TableConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, ChangeDataCaptureConfigurationBuilder> builder) {
      super(builder);
      this.attributes = TableConfiguration.attributeSet();
      this.pkb = new ColumnConfigurationBuilder();
      this.ccbList = new ArrayList<>();
   }

   public TableConfigurationBuilder name(String name) {
      attributes.attribute(TableConfiguration.TABLE_NAME).set(name);
      return this;
   }

   public ColumnConfigurationBuilder primaryKey() {
      return pkb;
   }

   public ColumnConfigurationBuilder addColumn() {
      ColumnConfigurationBuilder ccb = new ColumnConfigurationBuilder();
      ccbList.add(ccb);
      return ccb;
   }

   @Override
   public TableConfiguration create() {
      Collection<ColumnConfiguration> columns = ccbList.stream().map(ColumnConfigurationBuilder::create).toList();
      return new TableConfiguration(attributes.protect(), pkb.create(), columns);
   }

   @Override
   public Builder<?> read(TableConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      pkb.read(template.primaryKey(), combine);
      for (ColumnConfiguration column : template.columns()) {
         ColumnConfigurationBuilder ccb = new ColumnConfigurationBuilder();
         ccb.read(column, combine);
         ccbList.add(ccb);
      }
      return this;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public void validate() {
      pkb.validate();
      ccbList.forEach(Builder::validate);
   }
}
