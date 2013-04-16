package org.infinispan.loaders.jdbc;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

@Test(groups = "functional", testName = "loaders.jdbc.TableNameTest")
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
      assertEquals(tableName.getSchema(), "");
   }

   public void testSchema(){
      TableName tableName = new TableName(IDENTIFIER_QUOTE, "TEST.ISPN", "FOOBAR");
      assertEquals(tableName.getSchema(), "TEST");
      assertEquals(tableName.getName(), "ISPN_FOOBAR");
      assertEquals(tableName.toString(), "\"TEST\".\"ISPN_FOOBAR\"");

      tableName = new TableName(IDENTIFIER_QUOTE, "ISPN", "FOOBAR");
      assertEquals(tableName.getSchema(), null);
      assertEquals(tableName.getName(), "ISPN_FOOBAR");
      assertEquals(tableName.toString(), "\"ISPN_FOOBAR\"");
   }

   public void testName(){
      TableName tableName = new TableName(IDENTIFIER_QUOTE, "ISPN", "FOOBäR");
      assertEquals(tableName.toString(), "\"ISPN_FOOB_R\"");
   }
}
