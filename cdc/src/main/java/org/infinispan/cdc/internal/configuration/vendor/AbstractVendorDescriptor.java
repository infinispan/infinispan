package org.infinispan.cdc.internal.configuration.vendor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.cdc.internal.configuration.ForeignKey;
import org.infinispan.cdc.internal.configuration.PrimaryKey;

abstract class AbstractVendorDescriptor implements VendorDescriptor {

   protected PrimaryKey extractPrimaryKey(ResultSet rs) throws SQLException {
      String pkName = null;
      List<String> pkColumns = new ArrayList<>(2);
      while (rs.next()) {
         pkName = rs.getString(1);
         pkColumns.add(rs.getString(2).trim());
      }
      return new PrimaryKey(pkName, pkColumns);
   }

   protected List<ForeignKey> extractForeignKeys(ResultSet rs) throws SQLException {
      if (isScrollable(rs) && !rs.isBeforeFirst())
         return Collections.emptyList();

      List<ForeignKey> fks = new ArrayList<>(2);
      String fkName = null;
      List<String> columns = new ArrayList<>(2);
      String fkTable = null;
      List<String> fkColumns = new ArrayList<>(2);
      while (rs.next()) {
         String newFkName = rs.getString(1);
         if (fkName != null && !newFkName.equals(fkName)) {
            fks.add(new ForeignKey(fkName, columns, fkTable, fkColumns));
            columns = new ArrayList<>(2);
            fkColumns = new ArrayList<>(2);
         }
         fkName = newFkName;
         fkTable = rs.getString(2).trim();
         columns.add(rs.getString(3).trim());
         fkColumns.add(rs.getString(4).trim());
      }
      fks.add(new ForeignKey(fkName, columns, fkTable, fkColumns));
      return fks;
   }

   private boolean isScrollable(ResultSet rs) throws SQLException {
      return rs.getType() == ResultSet.TYPE_SCROLL_INSENSITIVE || rs.getType() == ResultSet.TYPE_SCROLL_SENSITIVE;
   }
}
