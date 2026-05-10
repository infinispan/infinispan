package org.infinispan.persistence.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.persistence.jdbc.impl.table.TableName;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.jdbc.TableNameTest")
public class TableNameTest {

   private static final String IDENTIFIER_QUOTE = "\"";

   @BeforeClass
   public void beforeClass(){

   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testNullType(){
      new TableName(null, "", "");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testNullPrefix(){
      new TableName(IDENTIFIER_QUOTE, null, "");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testNullName(){
      new TableName(IDENTIFIER_QUOTE, "", null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testEmptySchema(){
      TableName tableName = new TableName(IDENTIFIER_QUOTE, ".ISPN", "FOOBAR");
      assertEquals("", tableName.getSchema());
   }

   public void testSchema(){
      TableName tableName = new TableName(IDENTIFIER_QUOTE, "TEST.ISPN", "FOOBAR");
      assertEquals("TEST", tableName.getSchema());
      assertEquals("ISPN_FOOBAR", tableName.getName());
      assertEquals("\"TEST\".\"ISPN_FOOBAR\"", tableName.toString());

      tableName = new TableName(IDENTIFIER_QUOTE, "ISPN", "FOOBAR");
      assertNull(tableName.getSchema());
      assertEquals("ISPN_FOOBAR", tableName.getName());
      assertEquals("\"ISPN_FOOBAR\"", tableName.toString());
   }

   public void testName(){
      TableName tableName = new TableName(IDENTIFIER_QUOTE, "ISPN", "FOOBäR");
      assertEquals("\"ISPN_FOOB_R\"", tableName.toString());
   }
}
