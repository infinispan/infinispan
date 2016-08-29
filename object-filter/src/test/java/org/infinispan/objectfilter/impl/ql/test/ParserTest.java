/*
 * Copyright 2016, Red Hat Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.infinispan.objectfilter.impl.ql.test;

import org.junit.Test;

public class ParserTest extends TestBase {

   @Test
   public void testFromNote1() {
      expectParserSuccess("from Note n where ! n.text = 'foo'",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Note n))) (SELECT (SELECT_LIST (SELECT_ITEM n)))) " +
                  "(where (! (= (PATH (. n text)) (CONST_STRING_VALUE foo))))))");

      expectParserSuccess("from Note n where ! (n.text = 'foo')",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Note n))) (SELECT (SELECT_LIST (SELECT_ITEM n)))) " +
                  "(where (! (= (PATH (. n text)) (CONST_STRING_VALUE foo))))))");

      expectParserSuccess("from Note n where not n.text = 'foo'",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Note n))) (SELECT (SELECT_LIST (SELECT_ITEM n)))) " +
                  "(where (not (= (PATH (. n text)) (CONST_STRING_VALUE foo))))))");

      expectParserSuccess("from Note n where not (n.text = 'foo')",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Note n))) (SELECT (SELECT_LIST (SELECT_ITEM n)))) " +
                  "(where (not (= (PATH (. n text)) (CONST_STRING_VALUE foo))))))");
   }

   @Test
   public void testFromNote2() {
      expectParserSuccess("from Note where text : 'bar'",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Note <gen:0>))) (SELECT (SELECT_LIST (SELECT_ITEM <gen:0>)))) " +
                  "(where (: (PATH text) (FT_TERM (CONST_STRING_VALUE bar))))))");

      expectParserSuccess("from Note where #text : 'bar'",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Note <gen:0>))) (SELECT (SELECT_LIST (SELECT_ITEM <gen:0>)))) " +
                  "(where (: (PATH text) (# (FT_TERM (CONST_STRING_VALUE bar)))))))");

      expectParserSuccess("from Note where text : ('bar')",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Note <gen:0>))) (SELECT (SELECT_LIST (SELECT_ITEM <gen:0>)))) " +
                  "(where (: (PATH text) (FT_TERM (CONST_STRING_VALUE bar))))))");

      expectParserSuccess("from Note n where - n.text : (-'foo' +'bar')",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Note n))) (SELECT (SELECT_LIST (SELECT_ITEM n)))) " +
                  "(where (: (PATH (. n text)) (- (OR (- (FT_TERM (CONST_STRING_VALUE foo))) (+ (FT_TERM (CONST_STRING_VALUE bar)))))))))");

      expectParserSuccess("from Note n where not n.text : (-'foo' +'bar')",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Note n))) (SELECT (SELECT_LIST (SELECT_ITEM n)))) " +
                  "(where (not (: (PATH (. n text)) (OR (- (FT_TERM (CONST_STRING_VALUE foo))) (+ (FT_TERM (CONST_STRING_VALUE bar)))))))))");
   }

   @Test
   public void testFromNote3() {
      expectParserSuccess("from Note n where ! n.text : (-'foo' +'bar')",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Note n))) (SELECT (SELECT_LIST (SELECT_ITEM n)))) " +
                  "(where (! (: (PATH (. n text)) (OR (- (FT_TERM (CONST_STRING_VALUE foo))) (+ (FT_TERM (CONST_STRING_VALUE bar)))))))))");
   }

   @Test
   public void testFromNote4() {
      expectParserSuccess("from Note n where + n.text : 'foo'",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Note n))) (SELECT (SELECT_LIST (SELECT_ITEM n)))) " +
                  "(where (: (PATH (. n text)) (+ (FT_TERM (CONST_STRING_VALUE foo)))))))");
   }

   @Test
   public void testFromAnimal() {
      //generated alias:
      expectParserSuccess("from Animal",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Animal <gen:0>))) (SELECT (SELECT_LIST (SELECT_ITEM <gen:0>))))))");
   }

   @Test
   public void testSuperSimpleQuery() {
      //generated alias:
      expectParserSuccess("from EntityName",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF EntityName <gen:0>))) (SELECT (SELECT_LIST (SELECT_ITEM <gen:0>))))))");
   }

   @Test
   public void testSimpleQuery() {
      //full selection with specified alias:
      expectParserSuccess("select e from com.  acme   .  EntityName e",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF com.acme.EntityName e))) (select (SELECT_LIST (SELECT_ITEM (PATH e)))))))");
   }

   @Test
   public void testSimpleFromQuery() {
      //abbreviated form:
      expectParserSuccess("from com.acme.EntityName e",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF com.acme.EntityName e))) (SELECT (SELECT_LIST (SELECT_ITEM e))))))");
   }

   @Test
   public void testSimpleQueryDefaultContext() {
      //generated alias:
      expectParserSuccess("from com.acme.EntityName e",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF com.acme.EntityName e))) (SELECT (SELECT_LIST (SELECT_ITEM e))))))");
   }

   @Test
   public void testOneCriteriaQuery() {
      //generated alias:
      expectParserSuccess("from com.acme.EntityName e where e.name = 'same'",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF com.acme.EntityName e))) (SELECT (SELECT_LIST (SELECT_ITEM e)))) (where (= (PATH (. e name)) (CONST_STRING_VALUE same)))))");
   }

   @Test
   public void testOrderByAsc() {
      //generated alias:
      expectParserSuccess("from com.acme.EntityName e order by e.name asc",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF com.acme.EntityName e))) (SELECT (SELECT_LIST (SELECT_ITEM e))))) (order (SORT_SPEC (PATH (. e name)) asc)))");
   }

   @Test
   public void testOrderByDefault() {
      //generated alias:
      expectParserSuccess("from com.acme.EntityName e order by e.name",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF com.acme.EntityName e))) (SELECT (SELECT_LIST (SELECT_ITEM e))))) (order (SORT_SPEC (PATH (. e name)) asc)))");
   }

   @Test
   public void testOrderByDesc() {
      //generated alias:
      expectParserSuccess("from com.acme.EntityName e order by e.name DESC",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF com.acme.EntityName e))) (SELECT (SELECT_LIST (SELECT_ITEM e))))) (order (SORT_SPEC (PATH (. e name)) desc)))");
   }

   @Test
   public void testStringLiteralSingleQuoteEscape() {
      //generated alias:
      expectParserSuccess("from com.acme.EntityName e where e.name = 'Jack Daniel''s Old No. 7'",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF com.acme.EntityName e))) (SELECT (SELECT_LIST (SELECT_ITEM e)))) (where (= (PATH (. e name)) (CONST_STRING_VALUE Jack Daniel's Old No. 7)))))");
   }

   @Test
   public void testJoinOnEmbedded() {
      //generated alias:
      expectParserSuccess("SELECT e.author.name FROM IndexedEntity e JOIN e.contactDetails d  JOIN e.alternativeContactDetails a WHERE d.address.postCode='EA123' AND a.email='mail@mail.af'",
            "(QUERY (QUERY_SPEC (SELECT_FROM (FROM (PERSISTER_SPACE (ENTITY_PERSISTER_REF IndexedEntity e) (property-join INNER d (PATH (. e contactDetails))) (property-join INNER a (PATH (. e alternativeContactDetails))))) (SELECT (SELECT_LIST (SELECT_ITEM (PATH (. (. e author) name)))))) (WHERE (AND (= (PATH (. (. d address) postCode)) (CONST_STRING_VALUE EA123)) (= (PATH (. a email)) (CONST_STRING_VALUE mail@mail.af))))))");
   }

   @Test
   public void testProjectionOnEmbeddedAndUnqualifiedProperties() {
      //generated alias:
      expectParserSuccess("SELECT name, text, e.author.name FROM IndexedEntity e JOIN e.contactDetails d",
            "(QUERY (QUERY_SPEC (SELECT_FROM (FROM (PERSISTER_SPACE (ENTITY_PERSISTER_REF IndexedEntity e) (property-join INNER d (PATH (. e contactDetails))))) (SELECT (SELECT_LIST (SELECT_ITEM (PATH name)) (SELECT_ITEM (PATH text)) (SELECT_ITEM (PATH (. (. e author) name))))))))");
   }

   @Test
   public void testNamedParam() {
      expectParserSuccess("FROM IndexedEntity e where e.name = :nameParam",
            "(QUERY (QUERY_SPEC (SELECT_FROM (FROM (PERSISTER_SPACE (ENTITY_PERSISTER_REF IndexedEntity e))) (SELECT (SELECT_LIST (SELECT_ITEM e)))) (where (= (PATH (. e name)) nameParam))))");
   }
}
