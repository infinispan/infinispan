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

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class GrammarTest extends TestBase {

   @Test
   public void testFT1() {
      String expectedFrom = "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Cat cat))) (SELECT (SELECT_LIST (SELECT_ITEM cat)))) ";

      expectParserSuccess("from Cat cat where name = 1",
            expectedFrom + "(where (= (PATH name) 1))))");

      expectParserSuccess("from Cat cat where name : /tom.at/",
            expectedFrom + "(where (: (PATH name) (FT_REGEXP tom.at)))))");

      expectParserSuccess("from Cat cat where cat.name.xyz = 1",
            expectedFrom + "(where (= (PATH (. (. cat name) xyz)) 1))))");

      expectParserSuccess("from Cat cat where cat.name.xyz : 1",
            expectedFrom + "(where (: (PATH (. (. cat name) xyz)) (FT_TERM 1)))))");

      expectParserSuccess("from Cat cat where name : 'Tom*' and age > 5",
            expectedFrom + "(where (and (: (PATH name) (FT_TERM (CONST_STRING_VALUE Tom*))) (> (PATH age) 5)))))");

      expectParserFailure("from Cat cat where (name : 'Tom') + 2");

      expectParserFailure("from Cat cat where name : 'Tom' + 2");

      expectParserSuccess("from Cat cat where 2 > 5",
            expectedFrom + "(where (> 2 5))))");

      expectParserSuccess("from Cat cat where (2 > 5)", expectedFrom + "(where (> 2 5))))");

      expectParserSuccess("from Cat cat where name : (+'Tom')",
            expectedFrom + "(where (: (PATH name) (+ (FT_TERM (CONST_STRING_VALUE Tom)))))))");

      expectParserSuccess("from Cat cat where -name : (+1)",
            expectedFrom + "(where (: (PATH name) (- (+ (FT_TERM 1)))))))");

      expectParserSuccess("from Cat cat where t.description : (+'playful' -'fat' 'kitten')",
            expectedFrom + "(where (: (PATH (. t description)) (OR (OR (+ (FT_TERM (CONST_STRING_VALUE playful))) (- (FT_TERM (CONST_STRING_VALUE fat)))) (FT_TERM (CONST_STRING_VALUE kitten)))))))");
   }

   @Test
   public void testFT2() {
      String expectedFrom = "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Cat cat))) (SELECT (SELECT_LIST (SELECT_ITEM cat)))) ";

      expectParserSuccess("from Cat cat where name : ('xx') and not surname:9",
            expectedFrom + "(where (and (: (PATH name) (FT_TERM (CONST_STRING_VALUE xx))) (not (: (PATH surname) (FT_TERM 9)))))))");

      expectParserSuccess("from Cat cat where name : (not 9)",
            expectedFrom + "(where (: (PATH name) (not (FT_TERM 9))))))");

      expectParserSuccess("from Cat cat where name : 9",
            expectedFrom + "(where (: (PATH name) (FT_TERM 9)))))");

      expectParserSuccess("from Cat cat where not name : 9",
            expectedFrom + "(where (not (: (PATH name) (FT_TERM 9))))))");

      expectParserSuccess("from Cat cat where not name : (-9)",
            expectedFrom + "(where (not (: (PATH name) (- (FT_TERM 9)))))))");

      expectParserSuccess("from Cat cat where ! name : (-9)",
            expectedFrom + "(where (! (: (PATH name) (- (FT_TERM 9)))))))");

      expectParserSuccess("from Cat cat where -name : 9",
            expectedFrom + "(where (: (PATH name) (- (FT_TERM 9))))))");

      expectParserSuccess("from Cat cat where -name : (!9)",
            expectedFrom + "(where (: (PATH name) (- (! (FT_TERM 9)))))))");

      expectParserSuccess("from Cat cat where -name : (- 9)",
            expectedFrom + "(where (: (PATH name) (- (- (FT_TERM 9)))))))");

      expectParserSuccess("from Cat cat where -name : (-9)",
            expectedFrom + "(where (: (PATH name) (- (- (FT_TERM 9)))))))");

      expectParserSuccess("from Cat cat where -name : (-9 || 0)",
            expectedFrom + "(where (: (PATH name) (- (|| (- (FT_TERM 9)) (FT_TERM 0)))))))");

      expectParserSuccess("from Cat cat where -name : (+ 9)",
            expectedFrom + "(where (: (PATH name) (- (+ (FT_TERM 9)))))))");

      expectParserSuccess("from Cat cat where name : ('Fritz' 'Purr' 'Meow')",
            expectedFrom + "(where (: (PATH name) (OR (OR (FT_TERM (CONST_STRING_VALUE Fritz)) (FT_TERM (CONST_STRING_VALUE Purr))) (FT_TERM (CONST_STRING_VALUE Meow)))))))");

      expectParserSuccess("from Cat cat where name : ('Fritz' or 'Purr')",
            expectedFrom + "(where (: (PATH name) (or (FT_TERM (CONST_STRING_VALUE Fritz)) (FT_TERM (CONST_STRING_VALUE Purr)))))))");

      expectParserSuccess("from Cat cat where name : ('Fritz' and 'Purr')",
            expectedFrom + "(where (: (PATH name) (and (FT_TERM (CONST_STRING_VALUE Fritz)) (FT_TERM (CONST_STRING_VALUE Purr)))))))");

      expectParserSuccess("from Cat cat where name : \"Fritz\" and surname:'Purr'",
            expectedFrom + "(where (and (: (PATH name) (FT_TERM (CONST_STRING_VALUE Fritz))) (: (PATH surname) (FT_TERM (CONST_STRING_VALUE Purr)))))))");

      expectParserSuccess("from Cat cat where (name : \"Tom\") || name > 3",
            expectedFrom + "(where (|| (: (PATH name) (FT_TERM (CONST_STRING_VALUE Tom))) (> (PATH name) 3)))))");

      expectParserSuccess("from Cat cat where description : 'Tom Cat'~3 ^ 4",
            expectedFrom + "(where (: (PATH description) (4 (FT_TERM (CONST_STRING_VALUE Tom Cat) 3))))))");

      expectParserSuccess("from Cat cat where description : (:paramX)^7",
            expectedFrom + "(where (: (PATH description) (7 (FT_TERM paramX))))))");

      expectParserSuccess("from Cat cat where description : 44^7",
            expectedFrom + "(where (: (PATH description) (7 (FT_TERM 44))))))");
   }

   @Test
   public void testSpatial() {
      String expectedFrom = "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Cat cat))) (SELECT (SELECT_LIST (SELECT_ITEM cat)))) ";

      expectParserSuccess("from Cat cat where geofilt(location, 45.6, 39.4, 100)",
            expectedFrom + "(where (geofilt (PROPERTY_REFERENCE location) 45.6 39.4 100))))");

      expectParserSuccess("from Cat cat where geofilt(location, 45.6, 39.4, 100) order by geodist(location, 40.3, 30.99)",
            expectedFrom + "(where (geofilt (PROPERTY_REFERENCE location) 45.6 39.4 100))) (order (SORT_SPEC (geodist (PROPERTY_REFERENCE location) 40.3 30.99) asc)))");
   }

   @Test
   public void testFtRange() {
      String expectedFrom = "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Cat cat))) (SELECT (SELECT_LIST (SELECT_ITEM cat)))) ";

      expectParserSuccess("from Cat cat where description : ['aaa' to 'zzz']",
            expectedFrom + "(where (: (PATH description) (FT_RANGE [ (CONST_STRING_VALUE aaa) (CONST_STRING_VALUE zzz) ])))))");

      expectParserSuccess("from Cat cat where description : ['aaa' 'zzz']",
            expectedFrom + "(where (: (PATH description) (FT_RANGE [ (CONST_STRING_VALUE aaa) (CONST_STRING_VALUE zzz) ])))))");

      expectParserSuccess("from Cat cat where description : {'aaa' to 'zzz'}",
            expectedFrom + "(where (: (PATH description) (FT_RANGE { (CONST_STRING_VALUE aaa) (CONST_STRING_VALUE zzz) })))))");

      expectParserSuccess("from Cat cat where description : [* *]",
            expectedFrom + "(where (: (PATH description) (FT_RANGE [ * * ])))))");

      expectParserSuccess("from Cat cat where description : [* to 'xyz'}",
            expectedFrom + "(where (: (PATH description) (FT_RANGE [ * (CONST_STRING_VALUE xyz) })))))");

      expectParserSuccess("from Cat cat where description : {* to 'xyz']",
            expectedFrom + "(where (: (PATH description) (FT_RANGE { * (CONST_STRING_VALUE xyz) ])))))");

      expectParserSuccess("from Cat cat where description : {* to 'xyz'] ^ 8",
            expectedFrom + "(where (: (PATH description) (8 (FT_RANGE { * (CONST_STRING_VALUE xyz) ]))))))");
   }

   @Test
   public void testFtFuzzyAndProximity() {
      String expectedFrom = "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Cat cat))) (SELECT (SELECT_LIST (SELECT_ITEM cat)))) ";

      expectParserSuccess("from Cat cat where name : 'Tom' ~ 4",
            expectedFrom + "(where (: (PATH name) (FT_TERM (CONST_STRING_VALUE Tom) 4)))))");

      expectParserSuccess("from Cat cat where name : 'Tom' ~ 4 and surname='Doe'",
            expectedFrom + "(where (and (: (PATH name) (FT_TERM (CONST_STRING_VALUE Tom) 4)) (= (PATH surname) (CONST_STRING_VALUE Doe))))))");

      expectParserSuccess("from Cat cat where name : 'Tom'~",
            expectedFrom + "(where (: (PATH name) (FT_TERM (CONST_STRING_VALUE Tom) ~)))))");
   }

   @Test
   public void testParamsNotAllowedInSelect() {
      expectParserFailure("select :paramX from Bar b");
   }

   @Test
   public void testParamsNotAllowedInOrderBy() {
      expectParserFailure("from Bar order by :paramX");
   }

   @Test
   public void testParamsNotAllowedInGroupBy() {
      expectParserFailure("select max(b.diameter) from Bar b group by :paramX");
   }

   @Test
   public void testConstantBooleanInWhere() {
      expectParserSuccess("from example.EntityName e where true");
      expectParserSuccess("from example.EntityName e where false");
      expectParserSuccess("from example.EntityName e where false and e.name > 'a'");
      expectParserSuccess("from example.EntityName e where true and e.name > 'a'");
   }

   @Test
   public void testSimpleFrom_11() {
      expectParserSuccess("from example.EntityName e");
   }

   @Test
   public void testConstantUsage_13() {
      expectParserSuccess("from example.EntityName where prop = example.ParserTest.CONSTANT");
   }

   @Test
   public void testConstantUsage_14() {
      expectParserSuccess("from example.EntityName where prop = compProp.subProp");
   }

   @Test
   public void testVariousPropertyReferences_16() {
      expectParserSuccess("from A a where b = 1");
   }

   @Test
   public void testVariousPropertyReferences_17() {
      expectParserSuccess("from A a where a.b.c = 1");
   }

   @Test
   public void testVariousPropertyReferences_18() {
      expectParserSuccess("from X x where y[1].z = 2");
   }

   @Test
   public void testVariousPropertyReferences_19() {
      expectParserSuccess("from X x where x.y[1].z = 2");
   }

   @Test
   public void testEntityNamePathWithKeyword_21() {
      expectParserSuccess("from example.Inner");
   }

   @Test
   public void testWhereClauseIdentPrimaryWithEmbeddedKeyword_23() {
      expectParserSuccess("from example.Inner i where i.outer.inner.middle = 'xyz'");
   }

   @Test
   public void testDynamicInstantiation_26() {
      expectParserSuccess("from Animal join a.mate");
   }

   @Test
   public void testDynamicInstantiation_27() {
      expectParserSuccess("from Animal, aaa , dddd");
   }

   @Test
   public void testDynamicInstantiation_28() {
      expectParserSuccess("from Animal, aaa join fetch dddd");
   }

   @Test
   public void testListOrMapKeywordReference_30() {
      expectParserSuccess("select p from eg.NameList nl, eg.Person p where p.name = some elements(nl.names)");
   }

   @Test
   public void testListOrMapKeywordReference_31() {
      expectParserSuccess("select p from eg.NameList list, eg.Person p where p.name = some elements(list.names)");
   }

   @Test
   public void testListOrMapKeywordReference_32() {
      expectParserSuccess("select p from eg.NameList map, eg.Person p where p.name = some elements(map.names)");
   }

   @Test
   public void testExplicitPropertyJoin_34() {
      expectParserSuccess("from eg.Cat as cat inner join fetch cat.mate as m fetch all properties left join fetch cat.kittens as k");
   }

   @Test
   public void testSection_9_2_from_38() {
      expectParserSuccess("from eg.Cat");
   }

   @Test
   public void testSection_9_2_from_39() {
      expectParserSuccess("from eg.Cat as cat");
   }

   @Test
   public void testSection_9_2_from_40() {
      expectParserSuccess("from eg.Cat cat");
   }

   @Test
   public void testSection_9_2_from_41() {
      expectParserSuccess("from Formula, Parameter");
   }

   @Test
   public void testSection_9_2_from_42() {
      expectParserSuccess("from Formula as form, Parameter as param");
   }

   @Test
   public void testSection_9_3_Associations_and_joins_44() {
      expectParserSuccess("from eg.Cat as cat inner join cat.mate as mate left outer join cat.kittens as kitten");
   }

   @Test
   public void testSection_9_3_Associations_and_joins_45() {
      expectParserSuccess("from eg.Cat as cat left join cat.mate.kittens as kittens");
   }

   @Test
   public void testSection_9_3_Associations_and_joins_46() {
      expectParserSuccess("from Formula form full join form.parameter param");
   }

   @Test
   public void testSection_9_3_Associations_and_joins_47() {
      expectParserSuccess("from eg.Cat as cat join cat.mate as mate left join cat.kittens as kitten");
   }

   @Test
   public void testSection_9_3_Associations_and_joins_48() {
      expectParserSuccess("from eg.Cat as cat inner join fetch cat.mate left join fetch cat.kittens");
   }

   @Test
   public void testSection_9_4_Select_50() {
      expectParserSuccess("select mate from eg.Cat as cat inner join cat.mate as mate");
   }

   @Test
   public void testSection_9_4_Select_51() {
      expectParserSuccess("select cat.mate from eg.Cat cat");
   }

   @Test
   public void testSection_9_4_Select_52() {
      expectParserSuccess("select elements(cat.kittens) from eg.Cat cat");
   }

   @Test
   public void testSection_9_4_Select_53() {
      expectParserSuccess("select cat.name from eg.DomesticCat cat where cat.name like 'fri%'");
   }

   @Test
   public void testSection_9_4_Select_54() {
      expectParserSuccess("select cust.name.firstName from Customer as cust");
   }

   @Test
   public void testSection_9_4_Select_55() {
      expectParserSuccess("select mother, offspr, mate.name from eg.DomesticCat  as mother inner join mother.mate as mate left outer join mother.kittens as offspr");
   }

   @Test
   public void testSection_9_5_Aggregate_functions_58() {
      expectParserSuccess("select avg(cat.weight), sum(cat.weight), max(cat.weight), count(cat) from eg.Cat cat");
   }

   @Test
   public void testSection_9_5_Aggregate_functions_59() {
      expectParserSuccess("select cat, count( elements(cat.kittens) ) from eg.Cat cat group by cat");
   }

   @Test
   public void testSection_9_5_Aggregate_functions_60() {
      expectParserSuccess("select distinct cat.name from eg.Cat cat");
   }

   @Test
   public void testSection_9_5_Aggregate_functions_61() {
      expectParserSuccess("select count(distinct cat.name), count(cat) from eg.Cat cat");
   }

   @Test
   public void testSection_9_6_Polymorphism_63() {
      expectParserSuccess("from java.lang.Object o");
   }

   @Test
   public void testSection_9_6_Polymorphism_64() {
      expectParserSuccess("from eg.Named n, eg.Named m where n.name = m.name");
   }

   @Test
   public void testSection_9_7_Where_66() {
      expectParserSuccess("from eg.Cat as cat where cat.name='Fritz'");
   }

   @Test
   public void testSection_9_7_Where_67() {
      expectParserSuccess("select foo from eg.Foo foo, eg.Bar bar where foo.startDate = bar.date");
   }

   @Test
   public void testSection_9_7_Where_68() {
      expectParserSuccess("from eg.Cat cat where cat.mate.name is not null");
   }

   @Test
   public void testSection_9_7_Where_69() {
      expectParserSuccess("from eg.Cat cat, eg.Cat rival where cat.mate = rival.mate");
   }

   @Test
   public void testSection_9_7_Where_70() {
      expectParserSuccess("select cat, mate from eg.Cat cat, eg.Cat mate where cat.mate = mate");
   }

   @Test
   public void testSection_9_7_Where_71() {
      expectParserSuccess("from eg.Cat as cat where cat.id = 123");
   }

   @Test
   public void testSection_9_7_Where_72() {
      expectParserSuccess("from eg.Cat as cat where cat.mate.id = 69");
   }

   @Test
   public void testSection_9_7_Where_73() {
      expectParserSuccess("from bank.Person person where person.id.country = 'AU' and person.id.medicareNumber = 123456");
      expectParserSuccess("from bank.Person person where person.id.country = 'AU' && person.id.medicareNumber = 123456");
   }

   @Test
   public void testSection_9_7_Where_74() {
      expectParserSuccess("from bank.Account account where account.owner.id.country = 'AU' and account.owner.id.medicareNumber = 123456");
   }

   @Test
   public void testSection_9_7_Where_75() {
      expectParserSuccess("from eg.Cat cat where cat.class = eg.DomesticCat");
   }

   @Test
   public void testSection_9_7_Where_76() {
      expectParserSuccess("from eg.AuditLog log, eg.Payment payment where log.item.class = 'eg.Payment' and log.item.id = payment.id");
   }

   @Test
   public void testSection_9_8_Expressions_75() {
      expectParserSuccess("from eg.DomesticCat cat where cat.name between 'A' and 'B'",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF eg.DomesticCat cat))) (SELECT (SELECT_LIST (SELECT_ITEM cat)))) (where (between (PATH (. cat name)) (BETWEEN_LIST (CONST_STRING_VALUE A) (CONST_STRING_VALUE B))))))");
   }

   @Test
   public void testSection_9_7_Where_77() {
      expectParserFailure("from eg.Cat as cat where cat.name='Fritz' blah");
   }

   @Test
   public void testSection_9_7_Where_78() {
      expectParserFailure("from eg.Cat as cat where cat.name='Fritz' blah blah blah");
   }

   @Test
   public void testSection_9_7_Where_79() {
      expectParserFailure("from eg.Cat as cat where cat.name='Fritz' order by cat.age blah");
   }

   @Test
   public void testSection_9_7_Where_80() {
      expectParserFailure("from eg.Cat as cat where cat.name='Fritz' order by cat.age blah blah");
   }

   @Test
   public void testSection_9_8_Expressions_82() {
      expectParserSuccess("from eg.DomesticCat cat where cat.name between 'A' and 'B'");
   }

   @Test
   public void testSection_9_8_Expressions_83() {
      expectParserSuccess("from eg.DomesticCat cat where cat.name in ( 'Foo', 'Bar', 'Baz' )");
   }

   @Test
   public void testSection_9_8_Expressions_84() {
      expectParserSuccess("from eg.DomesticCat cat where cat.name not between 'A' and 'B'");
   }

   @Test
   public void testSection_9_8_Expressions_85() {
      expectParserSuccess("from eg.DomesticCat cat where cat.name not in ( 'Foo', 'Bar', 'Baz' )");
   }

   @Test
   public void testSection_9_8_Expressions_86() {
      expectParserSuccess("from eg.Cat cat where cat.kittens.size > 0");
   }

   @Test
   public void testSection_9_8_Expressions_87() {
      expectParserSuccess("from eg.Cat cat where size(cat.kittens) > 0");
   }

   @Test
   public void testSection_9_8_Expressions_88() {
      expectParserSuccess("from Order order where maxindex(order.items) > 100");
   }

   @Test
   public void testSection_9_8_Expressions_89() {
      expectParserSuccess("from Order order where minelement(order.items) > 10000");
   }

   @Test
   public void testSection_9_8_Expressions_90() {
      expectParserSuccess("from Order ord where maxindex(ord.items) > 100");
   }

   @Test
   public void testSection_9_8_Expressions_91() {
      expectParserSuccess("from Order ord where minelement(ord.items) > 10000");
   }

   @Test
   public void testSection_9_8_Expressions_92() {
      expectParserSuccess("select mother from eg.Cat as mother, eg.Cat as kit where kit in elements(foo.kittens)");
   }

   @Test
   public void testSection_9_8_Expressions_93() {
      expectParserSuccess("select p from eg.NameList list, eg.Person p where p.name = some elements(list.names)");
   }

   @Test
   public void testSection_9_8_Expressions_94() {
      expectParserSuccess("from eg.Cat cat where exists elements(cat.kittens)");
   }

   @Test
   public void testSection_9_8_Expressions_95() {
      expectParserSuccess("from eg.Player p where 3 > all elements(p.scores)");
   }

   @Test
   public void testSection_9_8_Expressions_96() {
      expectParserSuccess("from eg.Show show where 'fizard' in indices(show.acts)");
   }

   @Test
   public void testSection_9_8_Expressions_97() {
      expectParserSuccess("from Order order where order.items[0].id = 1234");
   }

   @Test
   public void testSection_9_8_Expressions_98() {
      expectParserSuccess("select person from Person person, Calendar calendar where calendar.holidays['national day'] = person.birthDay and person.nationality.calendar = calendar");
   }

   @Test
   public void testSection_9_8_Expressions_99() {
      expectParserSuccess("select item from Item item, Order order where order.items[ order.deliveredItemIndices[0] ] = item and order.id = 11");
   }

   @Test
   public void testSection_9_8_Expressions_100() {
      expectParserSuccess("select item from Item item, Order order where order.items[ maxindex(order.items) ] = item and order.id = 11");
   }

   @Test
   public void testSection_9_8_Expressions_101() {
      expectParserSuccess("from Order ord where ord.items[0].id = 1234");
   }

   @Test
   public void testSection_9_8_Expressions_102() {
      expectParserSuccess("select item from Item item, Order ord where ord.items[ ord.deliveredItemIndices[0] ] = item and ord.id = 11");
   }

   @Test
   public void testSection_9_8_Expressions_103() {
      expectParserSuccess("select item from Item item, Order ord where ord.items[ maxindex(ord.items) ] = item and ord.id = 11");
   }

   @Test
   public void testSection_9_8_Expressions_104() {
      expectParserSuccess("select item from Item item, Order ord where ord.items[ size(ord.items) ] = item");
   }

   @Test
   public void testSection_9_8_Expressions_106() {
      expectParserSuccess("select cust from Product prod, Store store inner join store.customers cust where prod.name = 'widget' and store.location.name in ( 'Melbourne', 'Sydney' ) and prod = all elements(cust.currentOrder.lineItems)");
   }

   @Test
   public void testDocoExamples99_108() {
      expectParserSuccess("from eg.DomesticCat cat order by cat.name asc, cat.weight desc, cat.birthdate");
   }

   @Test
   public void testDocoExamples910_110() {
      expectParserSuccess("select cat.color, sum(cat.weight), count(cat) from eg.Cat cat group by cat.color");
   }

   @Test
   public void testDocoExamples910_111() {
      expectParserSuccess("select foo.id, avg( elements(foo.names) ), max( indices(foo.names) ) from eg.Foo foo group by foo.id");
   }

   @Test
   public void testDocoExamples910_112() {
      expectParserSuccess("select cat.color, sum(cat.weight), count(cat) from eg.Cat cat group by cat.color having cat.color in (eg.Color.TABBY, eg.Color.BLACK)");
   }

   @Test
   public void testDocoExamples910_113() {
      expectParserSuccess("select cat from eg.Cat cat join cat.kittens kitten group by cat having avg(kitten.weight) > 100 order by count(kitten) asc, sum(kitten.weight) desc");
   }

   @Test
   public void testDocoExamples912_133() {
      expectParserSuccess("select ord.id, sum(price.amount), count(item)"
            + "  from Order as ord join ord.lineItems as item join item.product as product,"
            + "  Catalog as catalog join catalog.prices as price"
            + "  where ord.paid = false and ord.customer = :customer"
            + "  and price.product = product and catalog = :currentCatalog"
            + "  group by ord having sum(price.amount) > :minAmount"
            + "  order by sum(price.amount) desc");
   }

   @Test
   public void testDocoExamples912_155() {
      expectParserSuccess("select count(payment), status.name "
            + "  from Payment as payment"
            + "      join payment.currentStatus as status"
            + "  where payment.status.name <> PaymentStatus.AWAITING_APPROVAL"
            + "      or payment.statusChanges[ maxIndex(payment.statusChanges) ].user <> :currentUser"
            + "  group by status.name, status.sortOrder"
            + "  order by status.sortOrder");
   }

   @Test
   public void testDocoExamples912_162() {
      expectParserSuccess("select account, payment"
            + "  from Account as account"
            + "      left outer join account.payments as payment"
            + "  where :currentUser in elements(account.holder.users)"
            + "      and PaymentStatus.UNPAID = isNull(payment.currentStatus.name, PaymentStatus.UNPAID)"
            + "  order by account.type.sortOrder, account.accountNumber, payment.dueDate");
   }

   @Test
   public void testDocoExamples912_168() {
      expectParserSuccess("select account, payment"
            + "  from Account as account"
            + "      join account.holder.users as user"
            + "      left outer join account.payments as payment"
            + "  where :currentUser = user"
            + "      and PaymentStatus.UNPAID = isNull(payment.currentStatus.name, PaymentStatus.UNPAID)"
            + "  order by account.type.sortOrder, account.accountNumber, payment.dueDate");
   }

   @Test
   public void testExamples1_177() {
      expectParserSuccess("select s.name, sysdate, trunc(s.pay), round(s.pay) from Simple s");
   }

   @Test
   public void testExamples1_178() {
      expectParserSuccess("select abs(round(s.pay)) from Simple s");
   }

   @Test
   public void testMultipleActualParameters_181() {
      expectParserSuccess("select round(s.pay, 2) from s");
   }

   @Test
   public void testMultipleActualParameters_181_() {
      expectParserFailure("select round(, s.pay) from s");
   }

   @Test
   public void testMultipleActualParameters_181__() {
      expectParserSuccess("select round() from s");
   }

   @Test
   public void testMultipleFromClasses_183() {
      expectParserSuccess("from eg.mypackage.Cat qat, com.toadstool.Foo f");
   }

   @Test
   public void testMultipleFromClasses_184() {
      expectParserSuccess("from eg.mypackage.Cat qat, org.jabberwocky.Dipstick");
   }

   @Test
   public void testFromWithJoin_186() {
      expectParserSuccess("from eg.mypackage.Cat qat, com.toadstool.Foo f join net.sf.blurb.Blurb");
   }

   @Test
   public void testFromWithJoin_187() {
      expectParserSuccess("from eg.mypackage.Cat qat  left join com.multijoin.JoinORama , com.toadstool.Foo f join net.sf.blurb.Blurb");
   }

   @Test
   public void testSelect_189() {
      expectParserSuccess("select f from eg.mypackage.Cat qat, com.toadstool.Foo f join net.sf.blurb.Blurb");
   }

   @Test
   public void testSelect_189_() {
      expectParserFailure("from eg.mypackage.Cat order by :what");
   }

   @Test
   public void testSelect_190() {
      expectParserSuccess("select distinct bar from eg.mypackage.Cat qat  left join com.multijoin.JoinORama as bar, com.toadstool.Foo f join net.sf.blurb.Blurb");
   }

   @Test
   public void testSelect_191() {
      expectParserSuccess("select count(*) from eg.mypackage.Cat qat");
   }

   @Test
   public void testSelect_192() {
      expectParserSuccess("select avg(qat.weight) from eg.mypackage.Cat qat");
   }

   @Test
   public void testWhere_194() {
      expectParserSuccess("from eg.mypackage.Cat qat where qat.name like '%fluffy%' or qat.toes > 5");
   }

   @Test
   public void testWhere_195() {
      expectParserSuccess("from eg.mypackage.Cat qat where not qat.name like '%fluffy%' or qat.toes > 5");
   }

   @Test
   public void testWhere_196() {
      expectParserSuccess("from eg.mypackage.Cat qat where not qat.name not like '%fluffy%'");
   }

   @Test
   public void testWhere_197() {
      expectParserSuccess("from eg.mypackage.Cat qat where qat.name in ('crater','bean','fluffy')");
   }

   @Test
   public void testWhere_198() {
      expectParserSuccess("from eg.mypackage.Cat qat where qat.name not in ('crater','bean','fluffy')");
   }

   @Test
   public void testGroupBy_200() {
      expectParserSuccess("from eg.mypackage.Cat qat group by qat.breed");
   }

   @Test
   public void testGroupBy_201() {
      expectParserSuccess("from eg.mypackage.Cat qat group by qat.breed, qat.eyecolor");
   }

   @Test
   public void testOrderBy_203() {
      expectParserSuccess("from eg.mypackage.Cat qat order by avg(qat.toes)");
   }

   @Test
   public void testDoubleLiteral_206() {
      expectParserSuccess("from eg.Cat as tinycat where fatcat.weight < 3.1415");
   }

   @Test
   public void testDoubleLiteral_207() {
      expectParserSuccess("from eg.Cat as enormouscat where fatcat.weight > 3.1415e3");
   }

   @Test
   public void testInNotIn_212() {
      expectParserSuccess("from foo where foo.bar in ('a' , 'b', 'c')");
   }

   @Test
   public void testInNotIn_213() {
      expectParserSuccess("from foo where foo.bar not in ('a' , 'b', 'c')");
   }

   @Test
   public void testUnitTestHql_223() {
      expectParserSuccess("select foo.id from example.Foo foo where foo.joinedProp = 'foo'");
   }

   @Test
   public void testUnitTestHql_224() {
      expectParserSuccess("from example.Foo foo inner join fetch foo.foo");
   }

   @Test
   public void testUnitTestHql_225() {
      expectParserSuccess("from example.Baz baz left outer join fetch baz.fooToGlarch");
   }

   @Test
   public void testUnitTestHql_231() {
      expectParserSuccess("from example.Foo as foo where foo.component.glarch.name is not null");
   }

   @Test
   public void testUnitTestHql_232() {
      expectParserSuccess("from example.Foo as foo left outer join foo.component.glarch as glarch where glarch.name = 'foo'");
   }

   @Test
   public void testUnitTestHql_233() {
      expectParserSuccess("from example.Foo");
   }

   @Test
   public void testUnitTestHql_234() {
      expectParserSuccess("from example.Foo foo left outer join foo.foo");
   }

   @Test
   public void testUnitTestHql_235() {
      expectParserSuccess("from example.Foo, example.Bar");
   }

   @Test
   public void testUnitTestHql_236() {
      expectParserSuccess("from example.Baz baz left join baz.fooToGlarch, example.Bar bar join bar.foo");
   }

   @Test
   public void testUnitTestHql_237() {
      expectParserSuccess("from example.Baz baz left join baz.fooToGlarch join baz.fooSet");
   }

   @Test
   public void testUnitTestHql_238() {
      expectParserSuccess("from example.Baz baz left join baz.fooToGlarch join fetch baz.fooSet foo left join fetch foo.foo");
   }

   @Test
   public void testUnitTestHql_326() {
      expectParserSuccess("select index(date) from example.Baz baz join baz.stringDateMap as date");
   }

   @Test
   public void testUnitTestHql_327() {
      expectParserSuccess("select index(date) from example.Baz baz join baz.stringDateMap date");
   }

   @Test
   public void testUnitTestHql_329() {
      expectParserSuccess("from example.Baz baz inner join baz.collectionComponent.nested.foos foo where foo.string is null");
   }

   @Test
   public void testUnitTestHql_331() {
      expectParserSuccess("from example.Baz baz where 'a' in elements(baz.collectionComponent.nested.foos) and 1.0 in elements(baz.collectionComponent.nested.floats)");
   }

   @Test
   public void testUnitTestHql_332() {
      expectParserSuccess("from example.Foo foo join foo.foo where foo.foo in ('1','2','3')");
   }

   @Test
   public void testUnitTestHql_333() {
      expectParserSuccess("select foo.foo from example.Foo foo where foo.foo in ('1','2','3')");
   }

   @Test
   public void testUnitTestHql_334() {
      expectParserSuccess("select foo.foo.string from example.Foo foo where foo.foo in ('1','2','3')");
   }

   @Test
   public void testUnitTestHql_335() {
      expectParserSuccess("select foo.foo.string from example.Foo foo where foo.foo.string in ('1','2','3')");
   }

   @Test
   public void testUnitTestHql_336() {
      expectParserSuccess("select foo.foo.long from example.Foo foo where foo.foo.string in ('1','2','3')");
   }

   @Test
   public void testUnitTestHql_337() {
      expectParserSuccess("select count(*) from example.Foo foo where foo.foo.string in ('1','2','3') or foo.foo.long in (1,2,3)");
   }

   @Test
   public void testUnitTestHql_338() {
      expectParserSuccess("select count(*) from example.Foo foo where foo.foo.string in ('1','2','3') group by foo.foo.long");
   }

   @Test
   public void testUnitTestHql_339() {
      expectParserSuccess("from example.Foo foo1 left join foo1.foo foo2 left join foo2.foo where foo1.string is not null");
   }

   @Test
   public void testUnitTestHql_340() {
      expectParserSuccess("from example.Foo foo1 left join foo1.foo.foo where foo1.string is not null");
   }

   @Test
   public void testUnitTestHql_341() {
      expectParserSuccess("from example.Foo foo1 left join foo1.foo foo2 left join foo1.foo.foo foo3 where foo1.string is not null");
   }

   @Test
   public void testUnitTestHql_342() {
      expectParserSuccess("select foo.formula from example.Foo foo where foo.formula > 0");
   }

   @Test
   public void testUnitTestHql_343() {
      expectParserSuccess("from example.Foo as foo join foo.foo as foo2 where foo2.id >'a' or foo2.id <'a'");
   }

   @Test
   public void testUnitTestHql_344() {
      expectParserSuccess("from example.Holder");
   }

   @Test
   public void testUnitTestHql_345() {
      expectParserSuccess("from example.Baz baz left outer join fetch baz.manyToAny");
   }

   @Test
   public void testUnitTestHql_346() {
      expectParserSuccess("from example.Baz baz join baz.manyToAny");
   }

   @Test
   public void testUnitTestHql_347() {
      expectParserSuccess("select baz from example.Baz baz join baz.manyToAny a where index(a) = 0");
   }

   @Test
   public void testUnitTestHql_348() {
      expectParserSuccess("select bar from example.Bar bar where bar.baz.stringDateMap['now'] is not null");
   }

   @Test
   public void testUnitTestHql_349() {
      expectParserSuccess("select bar from example.Bar bar join bar.baaz b where b.stringDateMap['big bang'] < b.stringDateMap['now'] and b.stringDateMap['now'] is not null");
   }

   @Test
   public void testUnitTestHql_350() {
      expectParserSuccess("select bar from example.Bar bar where bar.baz.stringDateMap['big bang'] < bar.baz.stringDateMap['now'] and bar.baz.stringDateMap['now'] is not null");
   }

   @Test
   public void testUnitTestHql_351() {
      expectParserSuccess("select foo.string, foo.component, foo.id from example.Bar foo");
   }

   @Test
   public void testUnitTestHql_352() {
      expectParserSuccess("select elements(baz.components) from example.Baz baz");
   }

   @Test
   public void testUnitTestHql_353() {
      expectParserSuccess("select bc.name from example.Baz baz join baz.components bc");
   }

   @Test
   public void testUnitTestHql_354() {
      expectParserSuccess("from example.Foo foo where foo.integer < 10 order by foo.string");
   }

   @Test
   public void testUnitTestHql_355() {
      expectParserSuccess("from example.Fee");
   }

   @Test
   public void testUnitTestHql_356() {
      expectParserSuccess("from example.Holder h join h.otherHolder oh where h.otherHolder.name = 'bar'");
   }

   @Test
   public void testUnitTestHql_357() {
      expectParserSuccess("from example.Baz baz join baz.fooSet foo join foo.foo.foo foo2 where foo2.string = 'foo'");
   }

   @Test
   public void testUnitTestHql_358() {
      expectParserSuccess("from example.Baz baz join baz.fooArray foo join foo.foo.foo foo2 where foo2.string = 'foo'");
   }

   @Test
   public void testUnitTestHql_359() {
      expectParserSuccess("from example.Baz baz join baz.stringDateMap date where index(date) = 'foo'");
   }

   @Test
   public void testUnitTestHql_360() {
      expectParserSuccess("from example.Baz baz join baz.topGlarchez g where index(g) = 'A'");
   }

   @Test
   public void testUnitTestHql_361() {
      expectParserSuccess("select index(g) from example.Baz baz join baz.topGlarchez g");
   }

   @Test
   public void testUnitTestHql_362() {
      expectParserSuccess("from example.Baz baz left join baz.stringSet");
   }

   @Test
   public void testUnitTestHql_363() {
      expectParserSuccess("from example.Baz baz join baz.stringSet str where str='foo'");
   }

   @Test
   public void testUnitTestHql_364() {
      expectParserSuccess("from example.Baz baz left join fetch baz.stringSet");
   }

   @Test
   public void testUnitTestHql_365() {
      expectParserSuccess("from example.Baz baz join baz.stringSet string where string='foo'");
   }

   @Test
   public void testUnitTestHql_366() {
      expectParserSuccess("from example.Baz baz inner join baz.components comp where comp.name='foo'");
   }

   @Test
   public void testUnitTestHql_367() {
      expectParserSuccess("from example.Glarch g inner join g.fooComponents comp where comp.fee is not null");
   }

   @Test
   public void testUnitTestHql_368() {
      expectParserSuccess("from example.Glarch g inner join g.fooComponents comp join comp.fee fee where fee.count > 0");
   }

   @Test
   public void testUnitTestHql_369() {
      expectParserSuccess("from example.Glarch g inner join g.fooComponents comp where comp.fee.count is not null");
   }

   @Test
   public void testUnitTestHql_370() {
      expectParserSuccess("from example.Baz baz left join fetch baz.fooBag");
   }

   @Test
   public void testUnitTestHql_371() {
      expectParserSuccess("from example.Glarch");
   }

   @Test
   public void testUnitTestHql_372() {
      expectParserSuccess("from example.Baz baz left join fetch baz.sortablez order by baz.name asc");
   }

   @Test
   public void testUnitTestHql_373() {
      expectParserSuccess("from example.Baz baz order by baz.name asc");
   }

   @Test
   public void testUnitTestHql_374() {
      expectParserSuccess("from example.Foo foo, example.Baz baz left join fetch baz.fees");
   }

   @Test
   public void testUnitTestHql_375() {
      expectParserSuccess("from example.Foo foo, example.Bar bar");
   }

   @Test
   public void testUnitTestHql_376() {
      expectParserSuccess("from example.Foo foo");
   }

   @Test
   public void testUnitTestHql_377() {
      expectParserSuccess("from example.Foo foo, example.Bar bar, example.Bar bar2");
   }

   @Test
   public void testUnitTestHql_378() {
      expectParserSuccess("from example.X x");
   }

   @Test
   public void testUnitTestHql_379() {
      expectParserSuccess("select distinct foo from example.Foo foo");
   }

   @Test
   public void testUnitTestHql_380() {
      expectParserSuccess("from example.Glarch g where g.multiple.glarch=g and g.multiple.count=12");
   }

   @Test
   public void testUnitTestHql_381() {
      expectParserSuccess("from example.Bar bar left join bar.baz baz left join baz.cascadingBars b where bar.name like 'Bar %'");
   }

   @Test
   public void testUnitTestHql_382() {
      expectParserSuccess("select bar, b from example.Bar bar left join bar.baz baz left join baz.cascadingBars b where bar.name like 'Bar%'");
   }

   @Test
   public void testUnitTestHql_383() {
      expectParserSuccess("select bar, b from example.Bar bar left join bar.baz baz left join baz.cascadingBars b where ( bar.name in (:nameList0_, :nameList1_, :nameList2_) or bar.name in (:nameList0_, :nameList1_, :nameList2_) ) and bar.string = :stringVal");
   }

   @Test
   public void testUnitTestHql_384() {
      expectParserSuccess("select bar, b from example.Bar bar inner join bar.baz baz inner join baz.cascadingBars b where bar.name like 'Bar%'");
   }

   @Test
   public void testUnitTestHql_385() {
      expectParserSuccess("select bar, b from example.Bar bar left join bar.baz baz left join baz.cascadingBars b where bar.name like :name and b.name like :name");
   }

   @Test
   public void testUnitTestHql_386() {
      expectParserSuccess("select bar from example.Bar as bar where bar.x > ? or bar.short = 1 or bar.string = 'ff ? bb'");
   }

   @Test
   public void testUnitTestHql_387() {
      expectParserSuccess("select bar from example.Bar as bar where bar.string = ' ? ' or bar.string = '?'");
      expectParserSuccess("select bar from example.Bar as bar where bar.string = ' ? ' || bar.string = '?'");
   }

   @Test
   public void testUnitTestHql_388() {
      expectParserSuccess("from example.Baz baz, baz.fooArray foo");
   }

   @Test
   public void testUnitTestHql_398() {
      expectParserSuccess("select max( elements(bar.baz.fooArray) ) from example.Bar as bar");
   }

   @Test
   public void testUnitTestHql_399() {
      expectParserSuccess("from example.Baz baz left join baz.fooToGlarch join fetch baz.fooArray foo left join fetch foo.foo");
   }

   @Test
   public void testUnitTestHql_400() {
      expectParserSuccess("select baz.name from example.Bar bar inner join bar.baz baz inner join baz.fooSet foo where baz.name = bar.string");
   }

   @Test
   public void testUnitTestHql_401() {
      expectParserSuccess("SELECT baz.name FROM example.Bar AS bar INNER JOIN bar.baz AS baz INNER JOIN baz.fooSet AS foo WHERE baz.name = bar.string");
   }

   @Test
   public void testUnitTestHql_402() {
      expectParserSuccess("select baz.name from example.Bar bar join bar.baz baz left outer join baz.fooSet foo where baz.name = bar.string");
   }

   @Test
   public void testUnitTestHql_403() {
      expectParserSuccess("select baz.name from example.Bar bar, bar.baz baz, baz.fooSet foo where baz.name = bar.string");
   }

   @Test
   public void testUnitTestHql_404() {
      expectParserSuccess("SELECT baz.name FROM example.Bar AS bar, bar.baz AS baz, baz.fooSet AS foo WHERE baz.name = bar.string");
   }

   @Test
   public void testUnitTestHql_405() {
      expectParserSuccess("select baz.name from example.Bar bar left join bar.baz baz left join baz.fooSet foo where baz.name = bar.string");
   }

   @Test
   public void testUnitTestHql_406() {
      expectParserSuccess("select foo.string from example.Bar bar left join bar.baz.fooSet foo where bar.string = foo.string");
   }

   @Test
   public void testUnitTestHql_407() {
      expectParserSuccess("select baz.name from example.Bar bar left join bar.baz baz left join baz.fooArray foo where baz.name = bar.string");
   }

   @Test
   public void testUnitTestHql_408() {
      expectParserSuccess("select foo.string from example.Bar bar left join bar.baz.fooArray foo where bar.string = foo.string");
   }

   @Test
   public void testUnitTestHql_413() {
      expectParserSuccess("from example.Bar bar join bar.baz.fooArray foo");
   }

   @Test
   public void testUnitTestHql_426() {
      expectParserSuccess("from example.Foo foo where foo.component.glarch.id is not null");
   }

   @Test
   public void testUnitTestHql_430() {
      expectParserSuccess("select count(*) from example.Bar");
   }

   @Test
   public void testUnitTestHql_459() {
      expectParserSuccess("from example.Foo foo where foo.custom.s1 = 'one'");
   }

   @Test
   public void testUnitTestHql_466() {
      expectParserSuccess("from example.Bar bar where bar.object.id = ? and bar.object.class = ?");
   }

   @Test
   public void testUnitTestHql_467() {
      expectParserSuccess("select one from example.One one, example.Bar bar where bar.object.id = one.id and bar.object.class = 'O'");
   }

   @Test
   public void testUnitTestHql_469() {
      expectParserSuccess("from example.Bar bar");
   }

   @Test
   public void testUnitTestHql_470() {
      expectParserSuccess("From example.Bar bar");
   }

   @Test
   public void testUnitTestHql_471() {
      expectParserSuccess("From example.Foo foo");
   }

   @Test
   public void testUnitTestHql_475() {
      expectParserSuccess("from example.Outer o where o.id.detailId = ?");
   }

   @Test
   public void testUnitTestHql_476() {
      expectParserSuccess("from example.Outer o where o.id.master.id.sup.dudu is not null");
   }

   @Test
   public void testUnitTestHql_477() {
      expectParserSuccess("from example.Outer o where o.id.master.id.sup.id.akey is not null");
   }

   @Test
   public void testUnitTestHql_478() {
      expectParserSuccess("select o.id.master.id.sup.dudu from example.Outer o where o.id.master.id.sup.dudu is not null");
   }

   @Test
   public void testUnitTestHql_479() {
      expectParserSuccess("select o.id.master.id.sup.id.akey from example.Outer o where o.id.master.id.sup.id.akey is not null");
   }

   @Test
   public void testUnitTestHql_480() {
      expectParserSuccess("from example.Outer o where o.id.master.bla = ''");
   }

   @Test
   public void testUnitTestHql_481() {
      expectParserSuccess("from example.Outer o where o.id.master.id.one = ''");
   }

   @Test
   public void testUnitTestHql_482() {
      expectParserSuccess("from example.Inner inn where inn.id.bkey is not null and inn.backOut.id.master.id.sup.id.akey > 'a'");
   }

   @Test
   public void testUnitTestHql_483() {
      expectParserSuccess("from example.Outer as o left join o.id.master m left join m.id.sup where o.bubu is not null");
   }

   @Test
   public void testUnitTestHql_484() {
      expectParserSuccess("from example.Outer as o left join o.id.master.id.sup s where o.bubu is not null");
   }

   @Test
   public void testUnitTestHql_485() {
      expectParserSuccess("from example.Outer as o left join o.id.master m left join o.id.master.id.sup s where o.bubu is not null");
   }

   @Test
   public void testUnitTestHql_491() {
      expectParserSuccess("from example.Category cat where cat.name='new foo'");
   }

   @Test
   public void testUnitTestHql_492() {
      expectParserSuccess("from example.Category cat where cat.name='new sub'");
   }

   @Test
   public void testUnitTestHql_493() {
      expectParserSuccess("from example.Up up order by up.id2 asc");
   }

   @Test
   public void testUnitTestHql_494() {
      expectParserSuccess("from example.Down down");
   }

   @Test
   public void testUnitTestHql_495() {
      expectParserSuccess("from example.Up up");
   }

   @Test
   public void testUnitTestHql_511() {
      expectParserSuccess("select c from example.Container c where c.manyToMany[ c.oneToMany[0].count ].name = 's'");
   }

   @Test
   public void testUnitTestHql_512() {
      expectParserSuccess("select count(comp.name) from example.Container c join c.components comp");
   }

   @Test
   public void testUnitTestHql_513() {
      expectParserSuccess("from example.Parent p left join fetch p.child");
   }

   @Test
   public void testUnitTestHql_514() {
      expectParserSuccess("from example.Parent p join p.child c where c.x > 0");
   }

   @Test
   public void testUnitTestHql_515() {
      expectParserSuccess("from example.Child c join c.parent p where p.x > 0");
   }

   @Test
   public void testUnitTestHql_516() {
      expectParserSuccess("from example.Child");
   }

   @Test
   public void testUnitTestHql_517() {
      expectParserSuccess("from example.MoreStuff");
   }

   @Test
   public void testUnitTestHql_518() {
      expectParserSuccess("from example.Many");
   }

   @Test
   public void testUnitTestHql_519() {
      expectParserSuccess("from example.Qux");
   }

   @Test
   public void testUnitTestHql_520() {
      expectParserSuccess("from example.Fumm");
   }

   @Test
   public void testUnitTestHql_521() {
      expectParserSuccess("from example.Parent");
   }

   @Test
   public void testUnitTestHql_522() {
      expectParserSuccess("from example.Simple");
   }

   @Test
   public void testUnitTestHql_523() {
      expectParserSuccess("from example.Part");
   }

   @Test
   public void testUnitTestHql_524() {
      expectParserSuccess("from example.Baz");
   }

   @Test
   public void testUnitTestHql_525() {
      expectParserSuccess("from example.Vetoer");
   }

   @Test
   public void testUnitTestHql_526() {
      expectParserSuccess("from example.Sortable");
   }

   @Test
   public void testUnitTestHql_527() {
      expectParserSuccess("from example.Contained");
   }

   @Test
   public void testUnitTestHql_528() {
      expectParserSuccess("from example.Circular");
   }

   @Test
   public void testUnitTestHql_529() {
      expectParserSuccess("from example.Stuff");
   }

   @Test
   public void testUnitTestHql_530() {
      expectParserSuccess("from example.Immutable");
   }

   @Test
   public void testUnitTestHql_531() {
      expectParserSuccess("from example.Container");
   }

   @Test
   public void testUnitTestHql_532() {
      expectParserSuccess("from example.One");
   }

   @Test
   public void testUnitTestHql_533() {
      expectParserSuccess("from example.Fo");
   }

   @Test
   public void testUnitTestHql_534() {
      expectParserSuccess("from example.Glarch");
   }

   @Test
   public void testUnitTestHql_535() {
      expectParserSuccess("from example.Fum");
   }

   @Test
   public void testUnitTestHql_536() {
      expectParserSuccess("from example.Glarch g");
   }

   @Test
   public void testUnitTestHql_537() {
      expectParserSuccess("from example.Baz baz join baz.parts");
   }

   @Test
   public void testUnitTestHql_539() {
      expectParserSuccess("from example.Parent p join p.child c where p.count=66");
   }

   @Test
   public void testUnitTestHql_544() {
      expectParserSuccess("select count(*) from example.Container as c join c.components as ce join ce.simple as s where ce.name='foo'");
   }

   @Test
   public void testUnitTestHql_545() {
      expectParserSuccess("select c, s from example.Container as c join c.components as ce join ce.simple as s where ce.name='foo'");
   }

   @Test
   public void testUnitTestHql_555() {
      expectParserSuccess("from example.E e join e.reverse as b where b.count=1");
   }

   @Test
   public void testUnitTestHql_556() {
      expectParserSuccess("from example.E e join e.as as b where b.count=1");
   }

   @Test
   public void testUnitTestHql_557() {
      expectParserSuccess("from example.B");
   }

   @Test
   public void testUnitTestHql_558() {
      expectParserSuccess("from example.C1");
   }

   @Test
   public void testUnitTestHql_559() {
      expectParserSuccess("from example.C2");
   }

   @Test
   public void testUnitTestHql_560() {
      expectParserSuccess("from example.E e, example.A a where e.reverse = a.forward and a = ?");
   }

   @Test
   public void testUnitTestHql_561() {
      expectParserSuccess("from example.E e join fetch e.reverse");
   }

   @Test
   public void testUnitTestHql_562() {
      expectParserSuccess("from example.E e");
   }

   @Test
   public void testUnitTestHql_569() {
      expectParserSuccess("from example.Simple s where s.name=?");
   }

   @Test
   public void testUnitTestHql_570() {
      expectParserSuccess("from example.Simple s where s.name=:name");
   }

   @Test
   public void testUnitTestHql_587() {
      expectParserSuccess("from example.Simple s");
   }

   @Test
   public void testUnitTestHql_588() {
      expectParserSuccess("from example.Assignable");
   }

   @Test
   public void testUnitTestHql_589() {
      expectParserSuccess("from example.Category");
   }

   @Test
   public void testUnitTestHql_590() {
      expectParserSuccess("from example.A");
   }

   @Test
   public void testUnitTestHql_593() {
      expectParserSuccess("from example.Po po, example.Lower low where low.mypo = po");
   }

   @Test
   public void testUnitTestHql_594() {
      expectParserSuccess("from example.Po po join po.set as sm where sm.amount > 0");
   }

   @Test
   public void testUnitTestHql_595() {
      expectParserSuccess("from example.Po po join po.top as low where low.foo = 'po'");
   }

   @Test
   public void testUnitTestHql_596() {
      expectParserSuccess("from example.SubMulti sm join sm.children smc where smc.name > 'a'");
   }

   @Test
   public void testUnitTestHql_597() {
      expectParserSuccess("select s, ya from example.Lower s join s.yetanother ya");
   }

   @Test
   public void testUnitTestHql_598() {
      expectParserSuccess("from example.Lower s1 join s1.bag s2");
   }

   @Test
   public void testUnitTestHql_599() {
      expectParserSuccess("from example.Lower s1 left join s1.bag s2");
   }

   @Test
   public void testUnitTestHql_600() {
      expectParserSuccess("select s, a from example.Lower s join s.another a");
   }

   @Test
   public void testUnitTestHql_601() {
      expectParserSuccess("select s, a from example.Lower s left join s.another a");
   }

   @Test
   public void testUnitTestHql_602() {
      expectParserSuccess("from example.Top s, example.Lower ls");
   }

   @Test
   public void testUnitTestHql_603() {
      expectParserSuccess("from example.Lower ls join ls.set s where s.name > 'a'");
   }

   @Test
   public void testUnitTestHql_604() {
      expectParserSuccess("from example.Po po join po.list sm where sm.name > 'a'");
   }

   @Test
   public void testUnitTestHql_605() {
      expectParserSuccess("from example.Lower ls inner join ls.another s where s.name is not null");
   }

   @Test
   public void testUnitTestHql_606() {
      expectParserSuccess("from example.Lower ls where ls.other.another.name is not null");
   }

   @Test
   public void testUnitTestHql_607() {
      expectParserSuccess("from example.Multi m where m.derived like 'F%'");
   }

   @Test
   public void testUnitTestHql_608() {
      expectParserSuccess("from example.SubMulti m where m.derived like 'F%'");
   }

   @Test
   public void testUnitTestHql_609() {
      expectParserSuccess("select s from example.SubMulti as sm join sm.children as s where s.amount>-1 and s.name is null");
   }

   @Test
   public void testUnitTestHql_610() {
      expectParserSuccess("select elements(sm.children) from example.SubMulti as sm");
   }

   @Test
   public void testUnitTestHql_611() {
      expectParserSuccess("select distinct sm from example.SubMulti as sm join sm.children as s where s.amount>-1 and s.name is null");
   }

   @Test
   public void testUnitTestHql_638() {
      expectParserSuccess("from ChildMap cm where cm.parent is not null");
   }

   @Test
   public void testUnitTestHql_639() {
      expectParserSuccess("from ParentMap cm where cm.child is not null");
   }

   @Test
   public void testUnitTestHql_640() {
      expectParserSuccess("from example.Componentizable");
   }

   @Test
   public void testUnnamedParameter_642() {
      expectParserSuccess("select foo, bar from example.Foo foo left outer join foo.foo bar where foo = ?");
   }

   @Test
   public void testInElements_645() {
      expectParserSuccess("from example.Bar bar, foo in elements(bar.baz.fooArray)");
   }

   @Test
   public void testDotElements_650() {
      expectParserSuccess("from example.Baz baz where 'b' in elements(baz.collectionComponent.nested.foos) and 1.0 in elements(baz.collectionComponent.nested.floats)");
   }

   @Test
   public void testNot_652() {
      expectParserSuccess("from eg.Cat cat where not ( cat.kittens.size < 1 )");
   }

   @Test
   public void testNot_653() {
      expectParserSuccess("from eg.Cat cat where not ( cat.kittens.size > 1 )");
   }

   @Test
   public void testNot_654() {
      expectParserSuccess("from eg.Cat cat where not ( cat.kittens.size >= 1 )");
   }

   @Test
   public void testNot_655() {
      expectParserSuccess("from eg.Cat cat where not ( cat.kittens.size <= 1 )");
   }

   @Test
   public void testNot_656() {
      expectParserSuccess("from eg.DomesticCat cat where not ( cat.name between 'A' and 'B' )");
   }

   @Test
   public void testNot_657() {
      expectParserSuccess("from eg.DomesticCat cat where not ( cat.name not between 'A' and 'B' )");
   }

   @Test
   public void testNot_658() {
      expectParserSuccess("from eg.Cat cat where not ( not cat.kittens.size <= 1 )");
   }

   @Test
   public void testNot_659() {
      expectParserSuccess("from eg.Cat cat where not  not ( not cat.kittens.size <= 1 )");
   }

   @Test
   public void testOtherSyntax_661() {
      expectParserSuccess("select bar from example.Bar bar order by ((bar.x))");
   }

   @Test
   public void testOtherSyntax_662() {
      expectParserSuccess("from example.Bar bar, foo in elements(bar.baz.fooSet)");
   }

   @Test
   public void testOtherSyntax_664() {
      expectParserSuccess("from example.Inner _inner join _inner.middles middle");
   }

   @Test
   public void testEjbqlExtensions_675() {
      expectParserSuccess("select object(a) from Animal a where a.mother member of a.offspring");
   }

   @Test
   public void testEjbqlExtensions_676() {
      expectParserSuccess("select object(a) from Animal a where a.offspring is empty");
   }

   @Test
   public void testKeywordInPath_680() {
      expectParserSuccess("from Customer c where c.order.status = 'argh'");
   }

   @Test
   public void testKeywordInPath_681() {
      expectParserSuccess("from Customer c where c.order.count > 3");
   }

   @Test
   public void testKeywordInPath_682() {
      expectParserSuccess("select c.where from Customer c where c.order.count > 3");
   }

   @Test
   public void testKeywordInPath_683() {
      expectParserSuccess("from Interval i where i.end <:end");
   }

   @Test
   public void testKeywordInPath_684() {
      expectParserSuccess("from Letter l where l.case = :case");
   }

   @Test
   public void testPathologicalKeywordAsIdentifier_686() {
      expectParserSuccess("from Order order");
   }

   @Test
   public void testPathologicalKeywordAsIdentifier_687() {
      expectParserSuccess("from Order order join order.group");
   }

   @Test
   public void testPathologicalKeywordAsIdentifier_688() {
      expectParserSuccess("from X x order by x.group.by.from");
   }

   @Test
   public void testPathologicalKeywordAsIdentifier_689() {
      expectParserSuccess("from Order x order by x.order.group.by.from");
   }

   @Test
   public void testPathologicalKeywordAsIdentifier_690() {
      expectParserSuccess("select order.id from Order order");
   }

   @Test
   public void testPathologicalKeywordAsIdentifier_691() {
      expectParserSuccess("select order from Order order");
   }

   @Test
   public void testPathologicalKeywordAsIdentifier_692() {
      expectParserSuccess("from Order order where order.group.by.from is not null");
   }

   @Test
   public void testPathologicalKeywordAsIdentifier_693() {
      expectParserSuccess("from Order order order by order.group.by.from");
   }

   @Test
   public void testPathologicalKeywordAsIdentifier_694() {
      expectParserSuccess("from Group as group group by group.by.from");
   }

   @Test
   public void testHHH354_696() {
      expectParserSuccess("from Foo f where f.full = 'yep'");
   }

   @Test
   public void testWhereAsIdentifier_698() {
      expectParserSuccess("from where.Order");
   }

   @Test
   public void testMultiByteCharacters_702() {
      expectParserSuccess("from User user where user.name like '%nn\u4e2dnn%'");
   }

   @Test
   public void testHHH719_706() {
      expectParserSuccess("from Foo f order by com.fooco.SpecialFunction(f.id)");
   }

   @Test
   public void testHHH1107_708() {
      expectParserSuccess("from Animal where zoo.address.street = '123 Bogus St.'");
   }

   @Test
   public void testHHH1247_710() {
      expectParserSuccess("select distinct user.party from com.itf.iceclaims.domain.party.user.UserImpl user inner join user.party.$RelatedWorkgroups relatedWorkgroups where relatedWorkgroups.workgroup.id = :workgroup and relatedWorkgroups.effectiveTime.start <= :datesnow and relatedWorkgroups.effectiveTime.end > :dateenow ");
   }

   @Test
   public void testLineAndColumnNumber_712() {
      expectParserSuccess("from Foo f where f.name = 'fred'");
   }

   @Test
   public void testLineAndColumnNumber_713() {
      expectParserSuccess("from Animal a where a.bodyWeight = ?1");
   }

   @Test
   public void testLineAndColumnNumber_714() {
      expectParserSuccess("select object(m) from Model m");
   }

   @Test
   public void testLineAndColumnNumber_715() {
      expectParserSuccess("select o from Animal a inner join a.offspring o");
   }

   @Test
   public void testLineAndColumnNumber_716() {
      expectParserSuccess("select object(o) from Animal a, in(a.offspring) o");
   }

   @Test
   public void testLineAndColumnNumber_717() {
      expectParserSuccess("from Animal a where not exists elements(a.offspring)");
   }

   @Test
   public void testLineAndColumnNumber_719() {
      expectParserSuccess("select object(a) from Animal a where a.mother.father.offspring is not empty");
   }

   @Test
   public void testLineAndColumnNumber_722() {
      expectParserSuccess("select object(a) from Animal a where a.mother not member of a.offspring");
   }

   @Test
   public void testLineAndColumnNumber_723() {
      expectParserSuccess("select object(a) from Animal a where a.description = concat('1', concat('2','3'), '45')");
   }

   @Test
   public void testLineAndColumnNumber_724() {
      expectParserSuccess("from Animal a where substring(a.description, 1, 3) = :p1");
   }

   @Test
   public void testLineAndColumnNumber_725() {
      expectParserSuccess("select substring(a.description, 1, 3) from Animal a");
   }

   @Test
   public void testLineAndColumnNumber_728() {
      expectParserSuccess("from Animal a where length(a.description) = :p1");
   }

   @Test
   public void testLineAndColumnNumber_729() {
      expectParserSuccess("select length(a.description) from Animal a");
   }

   @Test
   public void testLineAndColumnNumber_730() {
      expectParserSuccess("from Animal a where locate(a.description, 'abc', 2) = :p1");
   }

   @Test
   public void testLineAndColumnNumber_731() {
      expectParserSuccess("select locate(a.description, :p1, 2) from Animal a");
   }

   @Test
   public void testLineAndColumnNumber_742() {
      expectParserSuccess("select object(a) from Animal a where a.bodyWeight like '%a%'");
   }

   @Test
   public void testLineAndColumnNumber_743() {
      expectParserSuccess("select object(a) from Animal a where a.bodyWeight not like '%a%'");
   }

   @Test
   public void testLineAndColumnNumber_744() {
      expectParserSuccess("select object(a) from Animal a where a.bodyWeight like '%a%' escape '%'");
   }

   @Test
   public void testLineAndColumnNumber_745() {
      expectParserFailure("from Human h where h.pregnant is true");
   }

   @Test
   public void testLineAndColumnNumber_746() {
      expectParserSuccess("from Human h where h.pregnant = true");
   }

   @Test
   public void testLineAndColumnNumber_747() {
      expectParserFailure("from Human h where h.pregnant is false");
   }

   @Test
   public void testLineAndColumnNumber_748() {
      expectParserSuccess("from Human h where h.pregnant = false");
   }

   @Test
   public void testLineAndColumnNumber_749() {
      expectParserFailure("from Human h where not(h.pregnant is true)");
   }

   @Test
   public void testLineAndColumnNumber_750() {
      expectParserSuccess("from Human h where not( h.pregnant=true )");
   }

   @Test
   public void testLineAndColumnNumber_754() {
      expectParserFailure("select * from Address a join Person p on a.pid = p.id, Person m join Address b on b.pid = m.id where p.mother = m.id and p.name like ?");
   }

   @Test
   public void testHQLTestInvalidCollectionDereferencesFail_757() {
      expectParserSuccess("from Animal a where a.offspring.description = 'xyz'");
   }

   @Test
   public void testHQLTestInvalidCollectionDereferencesFail_758() {
      expectParserSuccess("from Animal a where a.offspring.father.description = 'xyz'");
   }

   @Test
   public void testHQLTestSubComponentReferences_760() {
      expectParserSuccess("select c.address.zip.code from ComponentContainer c");
   }

   @Test
   public void testHQLTestSubComponentReferences_761() {
      expectParserSuccess("select c.address.zip from ComponentContainer c");
   }

   @Test
   public void testHQLTestSubComponentReferences_762() {
      expectParserSuccess("select c.address from ComponentContainer c");
   }

   @Test
   public void testHQLTestManyToAnyReferences_764() {
      expectParserSuccess("from PropertySet p where p.someSpecificProperty.id is not null");
   }

   @Test
   public void testHQLTestManyToAnyReferences_765() {
      expectParserSuccess("from PropertySet p join p.generalProperties gp where gp.id is not null");
   }

   @Test
   public void testHQLTestEmptyInListFailureExpected_770() {
      expectParserFailure("select a from Animal a where a.description in ()");
   }

   @Test
   public void testHQLTestEmptyInListFailureExpected_771() {
      expectParserSuccess("select o.orderDate from Order o");
   }

   @Test
   public void testHQLTestDateTimeArithmeticReturnTypesAndParameterGuessing_775() {
      expectParserSuccess("from Order o where o.orderDate > ?");
   }

   @Test
   public void testHQLTestReturnMetadata_778() {
      expectParserSuccess("select a as animal from Animal a");
   }

   @Test
   public void testHQLTestReturnMetadata_779() {
      expectParserSuccess("select o as entity from java.lang.Object o");
   }

   @Test
   public void testHQLTestImplicitJoinsAlongWithCartesianProduct_781() {
      expectParserSuccess("select foo.foo from Foo foo, Foo foo2");
   }

   @Test
   public void testHQLTestImplicitJoinsAlongWithCartesianProduct_782() {
      expectParserSuccess("select foo.foo.foo from Foo foo, Foo foo2");
   }

   @Test
   public void testHQLTestFetchOrderBy_790() {
      expectParserSuccess("from Animal a left outer join fetch a.offspring where a.mother.id = :mid order by a.description");
   }

   @Test
   public void testHQLTestCollectionOrderBy_792() {
      expectParserSuccess("from Animal a join a.offspring o order by a.description");
   }

   @Test
   public void testHQLTestCollectionOrderBy_793() {
      expectParserSuccess("from Animal a join fetch a.offspring order by a.description");
   }

   @Test
   public void testHQLTestCollectionOrderBy_794() {
      expectParserSuccess("from Animal a join fetch a.offspring o order by o.description");
   }

   @Test
   public void testHQLTestCollectionOrderBy_795() {
      expectParserSuccess("from Animal a join a.offspring o order by a.description, o.description");
   }

   @Test
   public void testHQLTestExpressionWithParamInFunction_797() {
      expectParserSuccess("from Animal a where abs(:param) < 2.0");
   }

   @Test
   public void testHQLTestExpressionWithParamInFunction_798() {
      expectParserSuccess("from Animal a where abs(a.bodyWeight) < 2.0");
   }

   @Test
   public void testHQLTestCompositeKeysWithPropertyNamedId_800() {
      expectParserSuccess("select e.id.id from EntityWithCrazyCompositeKey e");
   }

   @Test
   public void testHQLTestCompositeKeysWithPropertyNamedId_801() {
      expectParserSuccess("select max(e.id.id) from EntityWithCrazyCompositeKey e");
   }

   @Test
   public void testHQLTestMaxindexHqlFunctionInElementAccessorFailureExpected_803() {
      expectParserSuccess("select c from ContainerX c where c.manyToMany[ maxindex(c.manyToMany) ].count = 2");
   }

   @Test
   public void testHQLTestMaxindexHqlFunctionInElementAccessorFailureExpected_804() {
      expectParserSuccess("select c from Container c where c.manyToMany[ maxIndex(c.manyToMany) ].count = 2");
   }

   @Test
   public void testHQLTestMultipleElementAccessorOperatorsFailureExpected_806() {
      expectParserSuccess("select c from ContainerX c where c.oneToMany[ c.manyToMany[0].count ].name = 's'");
   }

   @Test
   public void testHQLTestKeyManyToOneJoinFailureExpected_808() {
      expectParserSuccess("from Order o left join fetch o.lineItems li left join fetch li.product p");
   }

   @Test
   public void testHQLTestKeyManyToOneJoinFailureExpected_809() {
      expectParserSuccess("from Outer o where o.id.master.id.sup.dudu is not null");
   }

   @Test
   public void testHQLTestDuplicateExplicitJoinFailureExpected_811() {
      expectParserSuccess("from Animal a join a.mother m1 join a.mother m2");
   }

   @Test
   public void testHQLTestDuplicateExplicitJoinFailureExpected_812() {
      expectParserSuccess("from Zoo zoo join zoo.animals an join zoo.mammals m");
   }

   @Test
   public void testHQLTestDuplicateExplicitJoinFailureExpected_813() {
      expectParserSuccess("from Zoo zoo join zoo.mammals an join zoo.mammals m");
   }

   @Test
   public void testHQLTestIndexWithExplicitJoin_815() {
      expectParserSuccess("from Zoo zoo join zoo.animals an where zoo.mammals[ index(an) ] = an");
   }

   @Test
   public void testHQLTestIndexWithExplicitJoin_816() {
      expectParserSuccess("from Zoo zoo join zoo.mammals dog where zoo.mammals[ index(dog) ] = dog");
   }

   @Test
   public void testHQLTestIndexWithExplicitJoin_817() {
      expectParserSuccess("from Zoo zoo join zoo.mammals dog where dog = zoo.mammals[ index(dog) ]");
   }

   @Test
   public void testHQLTestOneToManyMapIndex_819() {
      expectParserSuccess("from Zoo zoo where zoo.mammals['dog'].description like '%black%'");
   }

   @Test
   public void testHQLTestOneToManyMapIndex_820() {
      expectParserSuccess("from Zoo zoo where zoo.mammals['dog'].father.description like '%black%'");
   }

   @Test
   public void testHQLTestOneToManyMapIndex_821() {
      expectParserSuccess("from Zoo zoo where zoo.mammals['dog'].father.id = 1234");
   }

   @Test
   public void testHQLTestOneToManyMapIndex_822() {
      expectParserSuccess("from Zoo zoo where zoo.animals['1234'].description like '%black%'");
   }

   @Test
   public void testHQLTestExplicitJoinMapIndex_824() {
      expectParserSuccess("from Zoo zoo, Dog dog where zoo.mammals['dog'] = dog");
   }

   @Test
   public void testHQLTestExplicitJoinMapIndex_825() {
      expectParserSuccess("from Zoo zoo join zoo.mammals dog where zoo.mammals['dog'] = dog");
   }

   @Test
   public void testHQLTestIndexFunction_827() {
      expectParserSuccess("from Zoo zoo join zoo.mammals dog where index(dog) = 'dog'");
   }

   @Test
   public void testHQLTestIndexFunction_828() {
      expectParserSuccess("from Zoo zoo join zoo.animals an where index(an) = '1234'");
   }

   @Test
   public void testHQLTestSelectCollectionOfValues_830() {
      expectParserSuccess("select baz, date from Baz baz join baz.stringDateMap date where index(date) = 'foo'");
   }

   @Test
   public void testHQLTestCollectionOfValues_832() {
      expectParserSuccess("from Baz baz join baz.stringDateMap date where index(date) = 'foo'");
   }

   @Test
   public void testHQLTestHHH719_834() {
      expectParserSuccess("from Baz b order by org.bazco.SpecialFunction(b.id)");
   }

   @Test
   public void testHQLTestHHH719_835() {
      expectParserSuccess("from Baz b order by anypackage.anyFunction(b.id)");
   }

   @Test
   public void testHQLTestParameterListExpansion_837() {
      expectParserSuccess("from Animal as animal where animal.id in (:idList_1, :idList_2)");
   }

   @Test
   public void testHQLTestComponentManyToOneDereferenceShortcut_839() {
      expectParserSuccess("from Zoo z where z.address.stateProvince.id is null");
   }

   @Test
   public void testHQLTestNestedCollectionImplicitJoins_841() {
      expectParserSuccess("select h.friends.offspring from Human h");
   }

   @Test
   public void testHQLTestImplicitJoinsInGroupBy_845() {
      expectParserSuccess("select o.mother.bodyWeight, count(distinct o) from Animal an join an.offspring as o group by o.mother.bodyWeight");
   }

   @Test
   public void testHQLTestCrazyIdFieldNames_847() {
      expectParserSuccess("select e.heresAnotherCrazyIdFieldName from MoreCrazyIdFieldNameStuffEntity e where e.heresAnotherCrazyIdFieldName is not null");
   }

   @Test
   public void testHQLTestCrazyIdFieldNames_848() {
      expectParserSuccess("select e.heresAnotherCrazyIdFieldName.heresAnotherCrazyIdFieldName from MoreCrazyIdFieldNameStuffEntity e where e.heresAnotherCrazyIdFieldName is not null");
   }

   @Test
   public void testHQLTestSizeFunctionAndProperty_850() {
      expectParserSuccess("from Animal a where a.offspring.size > 0");
   }

   @Test
   public void testHQLTestSizeFunctionAndProperty_851() {
      expectParserSuccess("from Animal a join a.offspring where a.offspring.size > 1");
   }

   @Test
   public void testHQLTestSizeFunctionAndProperty_852() {
      expectParserSuccess("from Animal a where size(a.offspring) > 0");
   }

   @Test
   public void testHQLTestSizeFunctionAndProperty_853() {
      expectParserSuccess("from Animal a join a.offspring o where size(a.offspring) > 1");
   }

   @Test
   public void testHQLTestSizeFunctionAndProperty_854() {
      expectParserSuccess("from Animal a where size(a.offspring) > 1 and size(a.offspring) < 100");
   }

   @Test
   public void testHQLTestSizeFunctionAndProperty_855() {
      expectParserSuccess("from Human a where a.family.size > 0");
   }

   @Test
   public void testHQLTestSizeFunctionAndProperty_856() {
      expectParserSuccess("from Human a join a.family where a.family.size > 1");
   }

   @Test
   public void testHQLTestSizeFunctionAndProperty_857() {
      expectParserSuccess("from Human a where size(a.family) > 0");
   }

   @Test
   public void testHQLTestSizeFunctionAndProperty_858() {
      expectParserSuccess("from Human a join a.family o where size(a.family) > 1");
   }

   @Test
   public void testHQLTestSizeFunctionAndProperty_859() {
      expectParserSuccess("from Human a where a.family.size > 0 and a.family.size < 100");
   }

   @Test
   public void testHQLTestFromOnly_861() {
      expectParserSuccess("from Animal");
   }

   @Test
   public void testHQLTestJoinPathEndingInValueCollection_864() {
      expectParserSuccess("select h from Human as h join h.nickNames as nn where h.nickName=:nn1 and (nn=:nn2 or nn=:nn3)");
   }

   @Test
   public void testHQLTestSerialJoinPathEndingInValueCollection_866() {
      expectParserSuccess("select h from Human as h join h.friends as f join f.nickNames as nn where h.nickName=:nn1 and (nn=:nn2 or nn=:nn3)");
   }

   @Test
   public void testHQLTestImplicitJoinContainedByCollectionFunction_868() {
      expectParserSuccess("from Human as h where 'shipping' in indices(h.father.addresses)");
   }

   @Test
   public void testHQLTestImplicitJoinContainedByCollectionFunction_869() {
      expectParserSuccess("from Human as h where 'shipping' in indices(h.father.father.addresses)");
   }

   @Test
   public void testHQLTestImplicitJoinContainedByCollectionFunction_870() {
      expectParserSuccess("from Human as h where 'sparky' in elements(h.father.nickNames)");
   }

   @Test
   public void testHQLTestImplicitJoinContainedByCollectionFunction_871() {
      expectParserSuccess("from Human as h where 'sparky' in elements(h.father.father.nickNames)");
   }

   @Test
   public void testHQLTestCollectionOfValuesSize_877() {
      expectParserSuccess("select size(baz.stringDateMap) from example.legacy.Baz baz");
   }

   @Test
   public void testHQLTestCollectionFunctions_879() {
      expectParserSuccess("from Zoo zoo where size(zoo.animals) > 100");
   }

   @Test
   public void testHQLTestCollectionFunctions_880() {
      expectParserSuccess("from Zoo zoo where maxindex(zoo.mammals) = 'dog'");
   }

   @Test
   public void testHQLTestImplicitJoinInExplicitJoin_882() {
      expectParserSuccess("from Animal an inner join an.mother.mother gm");
   }

   @Test
   public void testHQLTestImplicitJoinInExplicitJoin_883() {
      expectParserSuccess("from Animal an inner join an.mother.mother.mother ggm");
   }

   @Test
   public void testHQLTestImplicitJoinInExplicitJoin_884() {
      expectParserSuccess("from Animal an inner join an.mother.mother.mother.mother gggm");
   }

   @Test
   public void testHQLTestImpliedManyToManyProperty_886() {
      expectParserSuccess("select c from ContainerX c where c.manyToMany[0].name = 's'");
   }

   @Test
   public void testHQLTestImpliedManyToManyProperty_887() {
      expectParserSuccess("select size(zoo.animals) from Zoo zoo");
   }

   @Test
   public void testHQLTestCollectionIndexFunctionsInSelect_889() {
      expectParserSuccess("select maxindex(zoo.animals) from Zoo zoo");
   }

   @Test
   public void testHQLTestCollectionIndexFunctionsInSelect_890() {
      expectParserSuccess("select minindex(zoo.animals) from Zoo zoo");
   }

   @Test
   public void testHQLTestCollectionIndexFunctionsInSelect_891() {
      expectParserSuccess("select indices(zoo.animals) from Zoo zoo");
   }

   @Test
   public void testHQLTestCollectionElementFunctionsInSelect_893() {
      expectParserSuccess("select maxelement(zoo.animals) from Zoo zoo");
   }

   @Test
   public void testHQLTestCollectionElementFunctionsInSelect_894() {
      expectParserSuccess("select minelement(zoo.animals) from Zoo zoo");
   }

   @Test
   public void testHQLTestCollectionElementFunctionsInSelect_895() {
      expectParserSuccess("select elements(zoo.animals) from Zoo zoo");
   }

   @Test
   public void testHQLTestFetchCollectionOfValues_897() {
      expectParserSuccess("from Baz baz left join fetch baz.stringSet");
   }

   @Test
   public void testHQLTestFetchList_899() {
      expectParserSuccess("from User u join fetch u.permissions");
   }

   @Test
   public void testHQLTestCollectionFetchWithExplicitThetaJoin_901() {
      expectParserSuccess("select m from Master m1, Master m left join fetch m.details where m.name=m1.name");
   }

   @Test
   public void testHQLTestListElementFunctionInSelect_903() {
      expectParserSuccess("select maxelement(u.permissions) from User u");
   }

   @Test
   public void testHQLTestListElementFunctionInSelect_904() {
      expectParserSuccess("select elements(u.permissions) from User u");
   }

   @Test
   public void testHQLTestListElementFunctionInWhere_906() {
      expectParserSuccess("from User u where 'read' in elements(u.permissions)");
   }

   @Test
   public void testHQLTestListElementFunctionInWhere_907() {
      expectParserSuccess("from User u where 'write' <> all elements(u.permissions)");
   }

   @Test
   public void testHQLTestManyToManyElementFunctionInSelect_909() {
      expectParserSuccess("select maxelement(human.friends) from Human human");
   }

   @Test
   public void testHQLTestManyToManyElementFunctionInSelect_910() {
      expectParserSuccess("select elements(human.friends) from Human human");
   }

   @Test
   public void testHQLTestManyToManyMaxElementFunctionInWhere_912() {
      expectParserSuccess("from Human human where 5 = maxelement(human.friends)");
   }

   @Test
   public void testHQLTestCollectionIndexFunctionsInWhere_914() {
      expectParserSuccess("from Zoo zoo where 4 = maxindex(zoo.animals)");
   }

   @Test
   public void testHQLTestCollectionIndexFunctionsInWhere_915() {
      expectParserSuccess("from Zoo zoo where 2 = minindex(zoo.animals)");
   }

   @Test
   public void testHQLTestCollectionIndicesInWhere_917() {
      expectParserSuccess("from Zoo zoo where 4 > some indices(zoo.animals)");
   }

   @Test
   public void testHQLTestCollectionIndicesInWhere_918() {
      expectParserSuccess("from Zoo zoo where 4 > all indices(zoo.animals)");
   }

   @Test
   public void testHQLTestIndicesInWhere_920() {
      expectParserSuccess("from Zoo zoo where 4 in indices(zoo.animals)");
   }

   @Test
   public void testHQLTestIndicesInWhere_921() {
      expectParserSuccess("from Zoo zoo where exists indices(zoo.animals)");
   }

   @Test
   public void testHQLTestCollectionElementInWhere_923() {
      expectParserSuccess("from Zoo zoo where 4 > some elements(zoo.animals)");
   }

   @Test
   public void testHQLTestCollectionElementInWhere_924() {
      expectParserSuccess("from Zoo zoo where 4 > all elements(zoo.animals)");
   }

   @Test
   public void testHQLTestElementsInWhere_926() {
      expectParserSuccess("from Zoo zoo where 4 in elements(zoo.animals)");
   }

   @Test
   public void testHQLTestElementsInWhere_927() {
      expectParserSuccess("from Zoo zoo where exists elements(zoo.animals)");
   }

   @Test
   public void testHQLTestNull_929() {
      expectParserSuccess("from Human h where h.nickName is null");
   }

   @Test
   public void testHQLTestNull_930() {
      expectParserSuccess("from Human h where h.nickName is not null");
   }

   @Test
   public void testHQLTestSubstitutions_932() {
      expectParserSuccess("from Human h where h.pregnant = yes");
   }

   @Test
   public void testHQLTestSubstitutions_933() {
      expectParserSuccess("from Human h where h.pregnant = foo");
   }

   @Test
   public void testHQLTestEscapedQuote_935() {
      expectParserSuccess("from Human h where h.nickName='1 ov''tha''few'");
   }

   @Test
   public void testHQLTestInvalidHql_941() {
      expectParserSuccess("from Animal foo where an.bodyWeight > 10");
   }

   @Test
   public void testHQLTestInvalidHql_942() {
      expectParserSuccess("select an.name from Animal foo");
   }

   @Test
   public void testHQLTestInvalidHql_943() {
      expectParserSuccess("from Animal foo where an.verybogus > 10");
   }

   @Test
   public void testHQLTestInvalidHql_944() {
      expectParserSuccess("select an.boguspropertyname from Animal foo");
   }

   @Test
   public void testHQLTestInvalidHql_945() {
      expectParserFailure("select an.name");
   }

   @Test
   public void testHQLTestInvalidHql_946() {
      expectParserFailure("from Animal an where (((an.bodyWeight > 10 and an.bodyWeight < 100)) or an.bodyWeight is null");
   }

   @Test
   public void testHQLTestInvalidHql_947() {
      expectParserFailure("from Animal an where an.bodyWeight is null where an.bodyWeight is null");
   }

   @Test
   public void testHQLTestInvalidHql_948() {
      expectParserFailure("from where name='foo'");
   }

   @Test
   public void testHQLTestInvalidHql_949() {
      expectParserSuccess("from NonexistentClass where name='foo'");
   }

   @Test
   public void testHQLTestWhereBetween_953() {
      expectParserSuccess("from Animal an where an.bodyWeight between 1 and 10");
   }

   @Test
   public void testHQLTestWhereLike_957() {
      expectParserSuccess("from Animal a where a.description like '%black%'");
   }

   @Test
   public void testHQLTestWhereLike_958() {
      expectParserSuccess("from Animal an where an.description like '%fat%'");
   }

   @Test
   public void testHQLTestWhereIn_961() {
      expectParserSuccess("from Animal an where an.description in ('fat', 'skinny')");
   }

   @Test
   public void testHQLTestLiteralInFunction_963() {
      expectParserSuccess("from Animal an where an.bodyWeight > abs(5)");
   }

   @Test
   public void testHQLTestLiteralInFunction_964() {
      expectParserSuccess("from Animal an where an.bodyWeight > abs(-5)");
   }

   @Test
   public void testHQLTestNotOrWhereClause_971() {
      expectParserSuccess("from Simple s where 'foo'='bar' or not 'foo'='foo'");
   }

   @Test
   public void testHQLTestNotOrWhereClause_972() {
      expectParserSuccess("from Simple s where 'foo'='bar' or not ('foo'='foo')");
   }

   @Test
   public void testHQLTestNotOrWhereClause_973() {
      expectParserSuccess("from Simple s where not ( 'foo'='bar' or 'foo'='foo' )");
   }

   @Test
   public void testHQLTestNotOrWhereClause_974() {
      expectParserSuccess("from Simple s where not ( 'foo'='bar' and 'foo'='foo' )");
   }

   @Test
   public void testHQLTestNotOrWhereClause_975() {
      expectParserSuccess("from Simple s where not ( 'foo'='bar' and 'foo'='foo' ) or not ('x'='y')");
   }

   @Test
   public void testHQLTestNotOrWhereClause_976() {
      expectParserSuccess("from Simple s where not ( 'foo'='bar' or 'foo'='foo' ) and not ('x'='y')");
   }

   @Test
   public void testHQLTestNotOrWhereClause_977() {
      expectParserSuccess("from Simple s where not ( 'foo'='bar' or 'foo'='foo' ) and 'x'='y'");
   }

   @Test
   public void testHQLTestNotOrWhereClause_978() {
      expectParserSuccess("from Simple s where not ( 'foo'='bar' and 'foo'='foo' ) or 'x'='y'");
   }

   @Test
   public void testHQLTestNotOrWhereClause_979() {
      expectParserSuccess("from Simple s where 'foo'='bar' and 'foo'='foo' or not 'x'='y'");
   }

   @Test
   public void testHQLTestNotOrWhereClause_980() {
      expectParserSuccess("from Simple s where 'foo'='bar' or 'foo'='foo' and not 'x'='y'");
   }

   @Test
   public void testHQLTestNotOrWhereClause_981() {
      expectParserSuccess("from Simple s where ('foo'='bar' and 'foo'='foo') or 'x'='y'");
   }

   @Test
   public void testHQLTestNotOrWhereClause_982() {
      expectParserSuccess("from Simple s where ('foo'='bar' or 'foo'='foo') and 'x'='y'");
   }

   @Test
   public void testHQLTestOrderBy_991() {
      expectParserSuccess("from Animal an order by an.bodyWeight");
   }

   @Test
   public void testHQLTestOrderBy_992() {
      expectParserSuccess("from Animal an order by an.bodyWeight asc");
   }

   @Test
   public void testHQLTestOrderBy_993() {
      expectParserSuccess("from Animal an order by an.bodyWeight desc");
   }

   @Test
   public void testHQLTestOrderBy_995() {
      expectParserSuccess("from Animal an order by an.mother.bodyWeight");
   }

   @Test
   public void testHQLTestOrderBy_996() {
      expectParserSuccess("from Animal an order by an.bodyWeight, an.description");
   }

   @Test
   public void testHQLTestOrderBy_997() {
      expectParserSuccess("from Animal an order by an.bodyWeight asc, an.description desc");
   }

   @Test
   public void testHQLTestGroupByFunction_1000() {
      expectParserSuccess("select count(*) from Human h group by year(h.birthdate)");
   }

   @Test
   public void testHQLTestGroupByFunction_1002() {
      expectParserSuccess("select count(*) from Human h group by year(sysdate)");
   }

   @Test
   public void testHQLTestPolymorphism_1004() {
      expectParserSuccess("from Mammal");
   }

   @Test
   public void testHQLTestPolymorphism_1005() {
      expectParserSuccess("from Dog");
   }

   @Test
   public void testHQLTestPolymorphism_1006() {
      expectParserSuccess("from Mammal m where m.pregnant = false and m.bodyWeight > 10");
   }

   @Test
   public void testHQLTestPolymorphism_1007() {
      expectParserSuccess("from Dog d where d.pregnant = false and d.bodyWeight > 10");
   }

   @Test
   public void testHQLTestProduct_1009() {
      expectParserSuccess("from Animal, Animal");
   }

   @Test
   public void testHQLTestProduct_1010() {
      expectParserSuccess("from Animal x, Animal y where x.bodyWeight = y.bodyWeight");
   }

   @Test
   public void testHQLTestProduct_1011() {
      expectParserSuccess("from Animal x, Mammal y where x.bodyWeight = y.bodyWeight and not y.pregnant = true");
   }

   @Test
   public void testHQLTestProduct_1012() {
      expectParserSuccess("from Mammal, Mammal");
   }

   @Test
   public void testHQLTestJoinedSubclassProduct_1014() {
      expectParserSuccess("from PettingZoo, PettingZoo");
   }

   @Test
   public void testHQLTestProjectProduct_1016() {
      expectParserSuccess("select x from Human x, Human y where x.nickName = y.nickName");
   }

   @Test
   public void testHQLTestProjectProduct_1017() {
      expectParserSuccess("select x, y from Human x, Human y where x.nickName = y.nickName");
   }

   @Test
   public void testHQLTestExplicitEntityJoins_1019() {
      expectParserSuccess("from Animal an inner join an.mother mo");
   }

   @Test
   public void testHQLTestExplicitEntityJoins_1020() {
      expectParserSuccess("from Animal an left outer join an.mother mo");
   }

   @Test
   public void testHQLTestExplicitEntityJoins_1021() {
      expectParserSuccess("from Animal an left outer join fetch an.mother");
   }

   @Test
   public void testHQLTestMultipleExplicitEntityJoins_1023() {
      expectParserSuccess("from Animal an inner join an.mother mo inner join mo.mother gm");
   }

   @Test
   public void testHQLTestMultipleExplicitEntityJoins_1024() {
      expectParserSuccess("from Animal an left outer join an.mother mo left outer join mo.mother gm");
   }

   @Test
   public void testHQLTestMultipleExplicitEntityJoins_1025() {
      expectParserSuccess("from Animal an inner join an.mother m inner join an.father f");
   }

   @Test
   public void testHQLTestMultipleExplicitEntityJoins_1026() {
      expectParserSuccess("from Animal an left join fetch an.mother m left join fetch an.father f");
   }

   @Test
   public void testHQLTestMultipleExplicitJoins_1028() {
      expectParserSuccess("from Animal an inner join an.mother mo inner join an.offspring os");
   }

   @Test
   public void testHQLTestMultipleExplicitJoins_1029() {
      expectParserSuccess("from Animal an left outer join an.mother mo left outer join an.offspring os");
   }

   @Test
   public void testHQLTestExplicitEntityJoinsWithRestriction_1031() {
      expectParserSuccess("from Animal an inner join an.mother mo where an.bodyWeight < mo.bodyWeight");
   }

   @Test
   public void testHQLTestIdProperty_1033() {
      expectParserSuccess("from Animal a where a.mother.id = 12");
   }

   @Test
   public void testHQLTestSubclassAssociation_1035() {
      expectParserSuccess("from DomesticAnimal da join da.owner o where o.nickName = 'Gavin'");
   }

   @Test
   public void testHQLTestSubclassAssociation_1036() {
      expectParserSuccess("from DomesticAnimal da left join fetch da.owner");
   }

   @Test
   public void testHQLTestSubclassAssociation_1037() {
      expectParserSuccess("from Human h join h.pets p where p.pregnant = 1");
   }

   @Test
   public void testHQLTestSubclassAssociation_1038() {
      expectParserSuccess("from Human h join h.pets p where p.bodyWeight > 100");
   }

   @Test
   public void testHQLTestSubclassAssociation_1039() {
      expectParserSuccess("from Human h left join fetch h.pets");
   }

   @Test
   public void testHQLTestExplicitCollectionJoins_1041() {
      expectParserSuccess("from Animal an inner join an.offspring os");
   }

   @Test
   public void testHQLTestExplicitCollectionJoins_1042() {
      expectParserSuccess("from Animal an left outer join an.offspring os");
   }

   @Test
   public void testHQLTestExplicitOuterJoinFetch_1044() {
      expectParserSuccess("from Animal an left outer join fetch an.offspring");
   }

   @Test
   public void testHQLTestExplicitOuterJoinFetchWithSelect_1046() {
      expectParserSuccess("select an from Animal an left outer join fetch an.offspring");
   }

   @Test
   public void testHQLTestExplicitJoins_1048() {
      expectParserSuccess("from Zoo zoo join zoo.mammals mam where mam.pregnant = true and mam.description like '%white%'");
   }

   @Test
   public void testHQLTestExplicitJoins_1049() {
      expectParserSuccess("from Zoo zoo join zoo.animals an where an.description like '%white%'");
   }

   @Test
   public void testHQLTestMultibyteCharacterConstant_1051() {
      expectParserSuccess("from Zoo zoo join zoo.animals an where an.description like '%\u4e2d%'");
   }

   @Test
   public void testHQLTestImplicitJoins_1053() {
      expectParserSuccess("from Animal an where an.mother.bodyWeight > ?");
   }

   @Test
   public void testHQLTestImplicitJoins_1054() {
      expectParserSuccess("from Animal an where an.mother.bodyWeight > 10");
   }

   @Test
   public void testHQLTestImplicitJoins_1055() {
      expectParserSuccess("from Dog dog where dog.mother.bodyWeight > 10");
   }

   @Test
   public void testHQLTestImplicitJoins_1056() {
      expectParserSuccess("from Animal an where an.mother.mother.bodyWeight > 10");
   }

   @Test
   public void testHQLTestImplicitJoins_1057() {
      expectParserSuccess("from Animal an where an.mother is not null");
   }

   @Test
   public void testHQLTestImplicitJoins_1058() {
      expectParserSuccess("from Animal an where an.mother.id = 123");
   }

   @Test
   public void testHQLTestImplicitJoinInSelect_1060() {
      expectParserSuccess("select foo, foo.long from Foo foo");
   }

   @Test
   public void testHQLTestImplicitJoinInSelect_1061() {
      expectParserSuccess("select foo.foo from Foo foo");
   }

   @Test
   public void testHQLTestImplicitJoinInSelect_1062() {
      expectParserSuccess("select foo, foo.foo from Foo foo");
   }

   @Test
   public void testHQLTestImplicitJoinInSelect_1063() {
      expectParserSuccess("select foo.foo from Foo foo where foo.foo is not null");
   }

   @Test
   public void testHQLTestSelectExpressions_1065() {
      expectParserSuccess("select an.mother.mother from Animal an");
   }

   @Test
   public void testHQLTestSelectExpressions_1066() {
      expectParserSuccess("select an.mother.mother.mother from Animal an");
   }

   @Test
   public void testHQLTestSelectExpressions_1067() {
      expectParserSuccess("select an.mother.mother.bodyWeight from Animal an");
   }

   @Test
   public void testHQLTestSelectExpressions_1068() {
      expectParserSuccess("select an.mother.zoo.id from Animal an");
   }

   @Test
   public void testHQLTestSelectExpressions_1069() {
      expectParserSuccess("select user.human.zoo.id from User user");
   }

   @Test
   public void testHQLTestSelectExpressions_1070() {
      expectParserSuccess("select u.userName, u.human.name.first from User u");
   }

   @Test
   public void testHQLTestSelectExpressions_1071() {
      expectParserSuccess("select u.human.name.last, u.human.name.first from User u");
   }

   @Test
   public void testHQLTestSelectExpressions_1072() {
      expectParserSuccess("select bar.baz.name from Bar bar");
   }

   @Test
   public void testHQLTestSelectExpressions_1073() {
      expectParserSuccess("select bar.baz.name, bar.baz.count from Bar bar");
   }

   @Test
   public void testHQLTestMapIndex_1077() {
      expectParserSuccess("from User u where u.permissions['datagrid']='read'");
   }

   @Test
   public void testHQLTestCollectionFunctionsInSelect_1079() {
      expectParserSuccess("select baz, size(baz.stringSet), count( distinct elements(baz.stringSet) ), max( elements(baz.stringSet) ) from Baz baz group by baz");
   }

   @Test
   public void testHQLTestCollectionFunctionsInSelect_1080() {
      expectParserSuccess("select elements(fum1.friends) from example.legacy.Fum fum1");
   }

   @Test
   public void testHQLTestCollectionFunctionsInSelect_1081() {
      expectParserSuccess("select elements(one.manies) from example.legacy.One one");
   }

   @Test
   public void testHQLTestNamedParameters_1083() {
      expectParserSuccess("from Animal an where an.mother.bodyWeight > :weight");
   }

   @Test
   public void testHQLTestClassProperty_1085() {
      expectParserSuccess("from Animal a where a.mother.class = Reptile");
   }

   @Test
   public void testHQLTestComponent_1087() {
      expectParserSuccess("from Human h where h.name.first = 'Gavin'");
   }

   @Test
   public void testHQLTestSelectEntity_1089() {
      expectParserSuccess("select an from Animal an inner join an.mother mo where an.bodyWeight < mo.bodyWeight");
   }

   @Test
   public void testHQLTestSelectEntity_1090() {
      expectParserSuccess("select mo, an from Animal an inner join an.mother mo where an.bodyWeight < mo.bodyWeight");
   }

   @Test
   public void testHQLTestValueAggregate_1092() {
      expectParserSuccess("select max(p), min(p) from User u join u.permissions p");
   }

   @Test
   public void testHQLTestAggregation_1094() {
      expectParserSuccess("select count(an) from Animal an");
   }

   @Test
   public void testHQLTestAggregation_1095() {
      expectParserSuccess("select count(distinct an) from Animal an");
   }

   @Test
   public void testHQLTestAggregation_1096() {
      expectParserSuccess("select count(distinct an.id) from Animal an");
   }

   @Test
   public void testHQLTestAggregation_1097() {
      expectParserSuccess("select count(all an.id) from Animal an");
   }

   @Test
   public void testHQLTestSelectProperty_1099() {
      expectParserSuccess("select an.bodyWeight, mo.bodyWeight from Animal an inner join an.mother mo where an.bodyWeight < mo.bodyWeight");
   }

   @Test
   public void testHQLTestSelectEntityProperty_1101() {
      expectParserSuccess("select an.mother from Animal an");
   }

   @Test
   public void testHQLTestSelectEntityProperty_1102() {
      expectParserSuccess("select an, an.mother from Animal an");
   }

   @Test
   public void testHQLTestSelectDistinctAll_1104() {
      expectParserSuccess("select distinct an.description, an.bodyWeight from Animal an");
   }

   @Test
   public void testHQLTestSelectDistinctAll_1105() {
      expectParserSuccess("select all an from Animal an");
   }

   @Test
   public void testHQLTestSelectAssociatedEntityId_1107() {
      expectParserSuccess("select an.mother.id from Animal an");
   }

   @Test
   public void testHQLTestGroupBy_1109() {
      expectParserSuccess("select an.mother.id, max(an.bodyWeight) from Animal an group by an.mother.id having max(an.bodyWeight)>1.0");
   }

   @Test
   public void testHQLTestGroupByMultiple_1111() {
      expectParserSuccess("select s.id, s.count, count(t), max(t.date) from example.legacy.Simple s, example.legacy.Simple t where s.count = t.count group by s.id, s.count order by s.count");
   }

   @Test
   public void testHQLTestManyToMany_1113() {
      expectParserSuccess("from Human h join h.friends f where f.nickName = 'Gavin'");
   }

   @Test
   public void testHQLTestManyToMany_1114() {
      expectParserSuccess("from Human h join h.friends f where f.bodyWeight > 100");
   }

   @Test
   public void testHQLTestManyToManyElementFunctionInWhere_1116() {
      expectParserSuccess("from Human human where human in elements(human.friends)");
   }

   @Test
   public void testHQLTestManyToManyElementFunctionInWhere_1117() {
      expectParserSuccess("from Human human where human = some elements(human.friends)");
   }

   @Test
   public void testHQLTestManyToManyElementFunctionInWhere2_1119() {
      expectParserSuccess("from Human h1, Human h2 where h2 in elements(h1.family)");
   }

   @Test
   public void testHQLTestManyToManyElementFunctionInWhere2_1120() {
      expectParserSuccess("from Human h1, Human h2 where 'father' in indices(h1.family)");
   }

   @Test
   public void testHQLTestManyToManyFetch_1122() {
      expectParserSuccess("from Human h left join fetch h.friends");
   }

   @Test
   public void testHQLTestManyToManyIndexAccessor_1124() {
      expectParserSuccess("select c from ContainerX c, Simple s where c.manyToMany[2] = s");
   }

   @Test
   public void testHQLTestManyToManyIndexAccessor_1125() {
      expectParserSuccess("select s from ContainerX c, Simple s where c.manyToMany[2] = s");
   }

   @Test
   public void testHQLTestManyToManyIndexAccessor_1126() {
      expectParserSuccess("from ContainerX c, Simple s where c.manyToMany[2] = s");
   }

   @Test
   public void testHQLTestPositionalParameters_1154() {
      expectParserSuccess("from Animal an where an.bodyWeight > ?");
   }

   @Test
   public void testHQLTestKeywordPropertyName_1156() {
      expectParserSuccess("from Glarch g order by g.order asc");
   }

   @Test
   public void testHQLTestKeywordPropertyName_1157() {
      expectParserSuccess("select g.order from Glarch g where g.order = 3");
   }

   @Test
   public void testHQLTestJavaConstant_1159() {
      expectParserSuccess("from example.legacy.Category c where c.name = example.legacy.Category.ROOT_CATEGORY");
   }

   @Test
   public void testHQLTestJavaConstant_1160() {
      expectParserSuccess("from example.legacy.Category c where c.id = example.legacy.Category.ROOT_ID");
   }

   @Test
   public void testHQLTestJavaConstant_1161() {
      expectParserSuccess("from Category c where c.name = Category.ROOT_CATEGORY");
   }

   @Test
   public void testHQLTestJavaConstant_1162() {
      expectParserSuccess("select c.name, Category.ROOT_ID from Category as c");
   }

   @Test
   public void testHQLTestClassName_1164() {
      expectParserSuccess("from Zoo zoo where zoo.class = PettingZoo");
   }

   @Test
   public void testHQLTestClassName_1165() {
      expectParserSuccess("from DomesticAnimal an where an.class = Dog");
   }

   @Test
   public void testHQLTestSelectDialectFunction_1170() {
      expectParserSuccess("select max(a.bodyWeight) from Animal a");
   }

   @Test
   public void testHQLTestTwoJoins_1172() {
      expectParserSuccess("from Human human join human.friends, Human h join h.mother");
   }

   @Test
   public void testHQLTestTwoJoins_1173() {
      expectParserSuccess("from Human human join human.friends f, Animal an join an.mother m where f=m");
   }

   @Test
   public void testHQLTestTwoJoins_1174() {
      expectParserSuccess("from Baz baz left join baz.fooToGlarch, Bar bar join bar.foo");
   }

   @Test
   public void testHQLTestToOneToManyManyJoinSequence_1176() {
      expectParserSuccess("from Dog d join d.owner h join h.friends f where f.name.first like 'joe%'");
   }

   @Test
   public void testHQLTestToOneToManyJoinSequence_1178() {
      expectParserSuccess("from Animal a join a.mother m join m.offspring");
   }

   @Test
   public void testHQLTestToOneToManyJoinSequence_1179() {
      expectParserSuccess("from Dog d join d.owner m join m.offspring");
   }

   @Test
   public void testHQLTestToOneToManyJoinSequence_1180() {
      expectParserSuccess("from Animal a join a.mother m join m.offspring o where o.bodyWeight > a.bodyWeight");
   }

   @Test
   public void testHQLTestSubclassExplicitJoin_1182() {
      expectParserSuccess("from DomesticAnimal da join da.owner o where o.nickName = 'gavin'");
   }

   @Test
   public void testHQLTestSubclassExplicitJoin_1183() {
      expectParserSuccess("from DomesticAnimal da join da.owner o where o.bodyWeight > 0");
   }

   @Test
   public void testHQLTestMultipleExplicitCollectionJoins_1185() {
      expectParserSuccess("from Animal an inner join an.offspring os join os.offspring gc");
   }

   @Test
   public void testHQLTestMultipleExplicitCollectionJoins_1186() {
      expectParserSuccess("from Animal an left outer join an.offspring os left outer join os.offspring gc");
   }

   @Test
   public void testHQLTestSelectDistinctComposite_1188() {
      expectParserSuccess("select distinct p from example.compositeelement.Parent p join p.children c where c.name like 'Child%'");
   }

   @Test
   public void testHQLTestDotComponent_1190() {
      expectParserSuccess("select fum.id from example.legacy.Fum as fum where not fum.fum='FRIEND'");
   }

   @Test
   public void testHQLTestOrderByCount_1192() {
      expectParserSuccess("from Animal an group by an.zoo.id order by an.zoo.id, count(*)");
   }

   @Test
   public void testHQLTestHavingCount_1194() {
      expectParserSuccess("from Animal an group by an.zoo.id having count(an.zoo.id) > 1");
   }

   @Test
   public void testHQLTest_selectWhereElements_1196() {
      expectParserSuccess("select foo from Foo foo, Baz baz where foo in elements(baz.fooArray)");
   }

   @Test
   public void testHQLTestCollectionOfComponents_1198() {
      expectParserSuccess("from Baz baz inner join baz.components comp where comp.name='foo'");
   }

   @Test
   public void testHQLTestOneToOneJoinedFetch_1200() {
      expectParserSuccess("from example.onetoone.joined.Person p join fetch p.address left join fetch p.mailingAddress");
   }

   @Test
   public void testHQLTestSubclassImplicitJoin_1202() {
      expectParserSuccess("from DomesticAnimal da where da.owner.nickName like 'Gavin%'");
   }

   @Test
   public void testHQLTestSubclassImplicitJoin_1203() {
      expectParserSuccess("from DomesticAnimal da where da.owner.nickName = 'gavin'");
   }

   @Test
   public void testHQLTestSubclassImplicitJoin_1204() {
      expectParserSuccess("from DomesticAnimal da where da.owner.bodyWeight > 0");
   }

   @Test
   public void testHQLTestComponent2_1206() {
      expectParserSuccess("from Dog dog where dog.owner.name.first = 'Gavin'");
   }

   @Test
   public void testHQLTestOneToOne_1208() {
      expectParserSuccess("from User u where u.human.nickName='Steve'");
   }

   @Test
   public void testHQLTestOneToOne_1209() {
      expectParserSuccess("from User u where u.human.name.first='Steve'");
   }

   @Test
   public void testHQLTestSelectClauseImplicitJoin_1211() {
      expectParserSuccess("select d.owner.mother from Dog d");
   }

   @Test
   public void testHQLTestSelectClauseImplicitJoin_1212() {
      expectParserSuccess("select d.owner.mother.description from Dog d");
   }

   @Test
   public void testHQLTestSelectClauseImplicitJoin_1213() {
      expectParserSuccess("select d.owner.mother from Dog d, Dog h");
   }

   @Test
   public void testHQLTestFromClauseImplicitJoin_1215() {
      expectParserSuccess("from DomesticAnimal da join da.owner.mother m where m.bodyWeight > 10");
   }

   @Test
   public void testHQLTestImplicitJoinInFrom_1217() {
      expectParserSuccess("from Human h join h.mother.mother.offspring o");
   }

   @Test
   public void testHQLTestDuplicateImplicitJoinInSelect_1219() {
      expectParserSuccess("select an.mother.bodyWeight from Animal an join an.mother m where an.mother.bodyWeight > 10");
   }

   @Test
   public void testHQLTestDuplicateImplicitJoinInSelect_1220() {
      expectParserSuccess("select an.mother.bodyWeight from Animal an where an.mother.bodyWeight > 10");
   }

   @Test
   public void testHQLTestDuplicateImplicitJoinInSelect_1221() {
      expectParserSuccess("select an.mother from Animal an where an.mother.bodyWeight is not null");
   }

   @Test
   public void testHQLTestDuplicateImplicitJoinInSelect_1222() {
      expectParserSuccess("select an.mother.bodyWeight from Animal an order by an.mother.bodyWeight");
   }

   @Test
   public void testHQLTestSelectProperty2_1224() {
      expectParserSuccess("select an, mo.bodyWeight from Animal an inner join an.mother mo where an.bodyWeight < mo.bodyWeight");
   }

   @Test
   public void testHQLTestSelectProperty2_1225() {
      expectParserSuccess("select an, mo, an.bodyWeight, mo.bodyWeight from Animal an inner join an.mother mo where an.bodyWeight < mo.bodyWeight");
   }

   @Test
   public void testHQLTestSubclassWhere_1227() {
      expectParserSuccess("from PettingZoo pz1, PettingZoo pz2 where pz1.id = pz2.id");
   }

   @Test
   public void testHQLTestSubclassWhere_1228() {
      expectParserSuccess("from PettingZoo pz1, PettingZoo pz2 where pz1.id = pz2");
   }

   @Test
   public void testHQLTestSubclassWhere_1229() {
      expectParserSuccess("from PettingZoo pz where pz.id > 0 ");
   }

   @Test
   public void testHQLTestNestedImplicitJoinsInSelect_1231() {
      expectParserSuccess("select foo.foo.foo.foo.string from example.legacy.Foo foo where foo.foo.foo = 'bar'");
   }

   @Test
   public void testHQLTestNestedImplicitJoinsInSelect_1232() {
      expectParserSuccess("select foo.foo.foo.foo.string from example.legacy.Foo foo");
   }

   @Test
   public void testHQLTestNestedComponent_1234() {
      expectParserSuccess("from example.legacy.Foo foo where foo.component.subcomponent.name='bar'");
   }

   @Test
   public void testHQLTestNull2_1236() {
      expectParserSuccess("from Human h where not( h.nickName is null )");
   }

   @Test
   public void testHQLTestNull2_1237() {
      expectParserSuccess("from Human h where not( h.nickName is not null )");
   }

   @Test
   public void testHQLTestUnknownFailureFromMultiTableTest_1239() {
      expectParserSuccess("from Lower s where s.yetanother.name='name'");
   }

   @Test
   public void testHQLTestJoinedSubclassImplicitJoin_1244() {
      expectParserSuccess("from example.legacy.Lower s where s.yetanother.name='name'");
   }

   @Test
   public void testHQLTestProjectProductJoinedSubclass_1246() {
      expectParserSuccess("select zoo from Zoo zoo, PettingZoo pz where zoo=pz");
   }

   @Test
   public void testHQLTestProjectProductJoinedSubclass_1247() {
      expectParserSuccess("select zoo, pz from Zoo zoo, PettingZoo pz where zoo=pz");
   }

   @Test
   public void testHQLTestFetch_1253() {
      expectParserSuccess("from Zoo zoo left join zoo.mammals");
   }

   @Test
   public void testHQLTestFetch_1254() {
      expectParserSuccess("from Zoo zoo left join fetch zoo.mammals");
   }

   @Test
   public void testHQLTestOneToManyElementFunctionInWhere_1256() {
      expectParserSuccess("from Zoo zoo where 'dog' in indices(zoo.mammals)");
   }

   @Test
   public void testHQLTestOneToManyElementFunctionInWhere_1257() {
      expectParserSuccess("from Zoo zoo, Dog dog where dog in elements(zoo.mammals)");
   }

   @Test
   public void testHQLTestManyToManyElementFunctionInSelect_1259() {
      expectParserSuccess("select elements(zoo.mammals) from Zoo zoo");
   }

   @Test
   public void testHQLTestManyToManyElementFunctionInSelect_1260() {
      expectParserSuccess("select indices(zoo.mammals) from Zoo zoo");
   }

   @Test
   public void testHQLTestManyToManyInJoin_1262() {
      expectParserSuccess("select x.id from Human h1 join h1.family x");
   }

   @Test
   public void testHQLTestManyToManyInJoin_1263() {
      expectParserSuccess("select index(h2) from Human h1 join h1.family h2");
   }

   @Test
   public void testHQLTestOneToManyIndexAccess_1267() {
      expectParserSuccess("from Zoo zoo where zoo.mammals['dog'] is not null");
   }

   @Test
   public void testHQLTestImpliedSelect_1269() {
      expectParserSuccess("select zoo from Zoo zoo");
   }

   @Test
   public void testHQLTestImpliedSelect_1270() {
      expectParserSuccess("from Zoo zoo");
   }

   @Test
   public void testHQLTestImpliedSelect_1271() {
      expectParserSuccess("from Zoo zoo join zoo.mammals m");
   }

   @Test
   public void testHQLTestImpliedSelect_1272() {
      expectParserSuccess("from Zoo");
   }

   @Test
   public void testHQLTestImpliedSelect_1273() {
      expectParserSuccess("from Zoo zoo join zoo.mammals");
   }

   @Test
   public void testHQLTestCollectionsInSelect2_1279() {
      expectParserSuccess("select foo.string from Bar bar left join bar.baz.fooArray foo where bar.string = foo.string");
   }

   @Test
   public void testHQLTestAssociationPropertyWithoutAlias_1281() {
      expectParserSuccess("from Animal where zoo is null");
   }

   @Test
   public void testHQLTestComponentNoAlias_1283() {
      expectParserSuccess("from Human where name.first = 'Gavin'");
   }

   @Test
   public void testASTParserLoadingTestComponentNullnessChecks_1285() {
      expectParserSuccess("from Human where name is null");
   }

   @Test
   public void testASTParserLoadingTestComponentNullnessChecks_1286() {
      expectParserSuccess("from Human where name is not null");
   }

   @Test
   public void testASTParserLoadingTestComponentNullnessChecks_1288() {
      expectParserSuccess("from Human where ? is null");
   }

   @Test
   public void testASTParserLoadingTestInvalidCollectionDereferencesFail_1290() {
      expectParserSuccess("from Animal a join a.offspring o where o.description = 'xyz'");
   }

   @Test
   public void testASTParserLoadingTestInvalidCollectionDereferencesFail_1291() {
      expectParserSuccess("from Animal a join a.offspring o where o.father.description = 'xyz'");
   }

   @Test
   public void testASTParserLoadingTestInvalidCollectionDereferencesFail_1292() {
      expectParserSuccess("from Animal a join a.offspring o order by o.description");
   }

   @Test
   public void testASTParserLoadingTestInvalidCollectionDereferencesFail_1293() {
      expectParserSuccess("from Animal a join a.offspring o order by o.father.description");
   }

   @Test
   public void testASTParserLoadingTestInvalidCollectionDereferencesFail_1294() {
      expectParserSuccess("from Animal a order by a.offspring.description");
   }

   @Test
   public void testASTParserLoadingTestInvalidCollectionDereferencesFail_1295() {
      expectParserSuccess("from Animal a order by a.offspring.father.description");
   }

   @Test
   public void testASTParserLoadingTestCrazyIdFieldNames_1308() {
      expectParserSuccess("select e.heresAnotherCrazyIdFieldName from MoreCrazyIdFieldNameStuffEntity e");
   }

   @Test
   public void testASTParserLoadingTestImplicitJoinsInDifferentClauses_1310() {
      expectParserSuccess("select e.owner from SimpleAssociatedEntity e");
   }

   @Test
   public void testASTParserLoadingTestImplicitJoinsInDifferentClauses_1311() {
      expectParserSuccess("select e.id, e.owner from SimpleAssociatedEntity e");
   }

   @Test
   public void testASTParserLoadingTestImplicitJoinsInDifferentClauses_1312() {
      expectParserSuccess("from SimpleAssociatedEntity e order by e.owner");
   }

   @Test
   public void testASTParserLoadingTestImplicitJoinsInDifferentClauses_1313() {
      expectParserSuccess("select e.owner.id, count(*) from SimpleAssociatedEntity e group by e.owner");
   }

   @Test
   public void testASTParserLoadingTestNestedComponentIsNull_1315() {
      expectParserSuccess("from Commento c where c.marelo.commento.mcompr is null");
   }

   @Test
   public void testASTParserLoadingTestSpecialClassPropertyReference_1317() {
      expectParserSuccess("select a.description from Animal a where a.class = Mammal");
   }

   @Test
   public void testASTParserLoadingTestSpecialClassPropertyReference_1318() {
      expectParserSuccess("select a.class from Animal a");
   }

   @Test
   public void testASTParserLoadingTestSpecialClassPropertyReference_1319() {
      expectParserSuccess("from Animal an where an.class = Dog");
   }

   @Test
   public void testASTParserLoadingTestSpecialClassPropertyReferenceFQN_1321() {
      expectParserSuccess("from Zoo zoo where zoo.class = example.PettingZoo");
   }

   @Test
   public void testASTParserLoadingTestSpecialClassPropertyReferenceFQN_1322() {
      expectParserSuccess("select a.description from Animal a where a.class = example.Mammal");
   }

   @Test
   public void testASTParserLoadingTestSpecialClassPropertyReferenceFQN_1323() {
      expectParserSuccess("from DomesticAnimal an where an.class = example.Dog");
   }

   @Test
   public void testASTParserLoadingTestSpecialClassPropertyReferenceFQN_1324() {
      expectParserSuccess("from Animal an where an.class = example.Dog");
   }

   @Test
   public void testASTParserLoadingTestSubclassOrSuperclassPropertyReferenceInJoinedSubclass_1326() {
      expectParserSuccess("from Zoo z join z.mammals as m where m.name.first = 'John'");
   }

   @Test
   public void testASTParserLoadingTestSubclassOrSuperclassPropertyReferenceInJoinedSubclass_1327() {
      expectParserSuccess("from Zoo z join z.mammals as m where m.pregnant = false");
   }

   @Test
   public void testASTParserLoadingTestSubclassOrSuperclassPropertyReferenceInJoinedSubclass_1328() {
      expectParserSuccess("select m.pregnant from Zoo z join z.mammals as m where m.pregnant = false");
   }

   @Test
   public void testASTParserLoadingTestSubclassOrSuperclassPropertyReferenceInJoinedSubclass_1329() {
      expectParserSuccess("from Zoo z join z.mammals as m where m.description = 'tabby'");
   }

   @Test
   public void testASTParserLoadingTestSubclassOrSuperclassPropertyReferenceInJoinedSubclass_1330() {
      expectParserSuccess("select m.description from Zoo z join z.mammals as m where m.description = 'tabby'");
   }

   @Test
   public void testASTParserLoadingTestSubclassOrSuperclassPropertyReferenceInJoinedSubclass_1331() {
      expectParserSuccess("select m.name from Zoo z join z.mammals as m where m.name.first = 'John'");
   }

   @Test
   public void testASTParserLoadingTestSubclassOrSuperclassPropertyReferenceInJoinedSubclass_1332() {
      expectParserSuccess("select m.pregnant from Zoo z join z.mammals as m");
   }

   @Test
   public void testASTParserLoadingTestSubclassOrSuperclassPropertyReferenceInJoinedSubclass_1333() {
      expectParserSuccess("select m.description from Zoo z join z.mammals as m");
   }

   @Test
   public void testASTParserLoadingTestSubclassOrSuperclassPropertyReferenceInJoinedSubclass_1334() {
      expectParserSuccess("select m.name from Zoo z join z.mammals as m");
   }

   @Test
   public void testASTParserLoadingTestSubclassOrSuperclassPropertyReferenceInJoinedSubclass_1335() {
      expectParserSuccess("from DomesticAnimal da join da.owner as o where o.nickName = 'Gavin'");
   }

   @Test
   public void testASTParserLoadingTestSubclassOrSuperclassPropertyReferenceInJoinedSubclass_1336() {
      expectParserSuccess("select da.father from DomesticAnimal da join da.owner as o where o.nickName = 'Gavin'");
   }

   @Test
   public void testASTParserLoadingTestJPAPositionalParameterList_1338() {
      expectParserSuccess("from Human where name.last in (?1)");
   }

   @Test
   public void testASTParserLoadingTestComponentQueries_1340() {
      expectParserSuccess("select h.name from Human h");
   }

   @Test
   public void testASTParserLoadingTestComponentQueries_1341() {
      expectParserSuccess("from Human h where h.name = h.name");
   }

   @Test
   public void testASTParserLoadingTestComponentQueries_1342() {
      expectParserSuccess("from Human h where h.name = :name");
   }

   @Test
   public void testASTParserLoadingTestComponentQueries_1343() {
      expectParserSuccess("from Human where name = :name");
   }

   @Test
   public void testASTParserLoadingTestComponentQueries_1344() {
      expectParserSuccess("from Human h where :name = h.name");
   }

   @Test
   public void testASTParserLoadingTestComponentQueries_1345() {
      expectParserSuccess("from Human h where :name <> h.name");
   }

   @Test
   public void testASTParserLoadingTestComponentQueries_1346() {
      expectParserSuccess("from Human h where h.name = ('John', 'X', 'Doe')");
   }

   @Test
   public void testASTParserLoadingTestComponentQueries_1347() {
      expectParserSuccess("from Human h where ('John', 'X', 'Doe') = h.name");
   }

   @Test
   public void testASTParserLoadingTestComponentQueries_1348() {
      expectParserSuccess("from Human h where ('John', 'X', 'Doe') <> h.name");
   }

   @Test
   public void testASTParserLoadingTestComponentQueries_1349() {
      expectParserSuccess("from Human h where ('John', 'X', 'Doe') >= h.name");
   }

   @Test
   public void testASTParserLoadingTestComponentQueries_1350() {
      expectParserSuccess("from Human h order by h.name");
   }

   @Test
   public void testASTParserLoadingTestComponentParameterBinding_1352() {
      expectParserSuccess("from Order o where o.customer.name =:name and o.id = :id");
   }

   @Test
   public void testASTParserLoadingTestComponentParameterBinding_1353() {
      expectParserSuccess("from Order o where o.id = :id and o.customer.name =:name ");
   }

   @Test
   public void testASTParserLoadingTestAnyMappingReference_1355() {
      expectParserSuccess("from PropertySet p where p.someSpecificProperty = :ssp");
   }

   @Test
   public void testASTParserLoadingTestJdkEnumStyleEnumConstant_1357() {
      expectParserSuccess("from Zoo z where z.classification = example.Classification.LAME");
   }

   @Test
   public void testASTParserLoadingTestParameterTypeMismatchFailureExpected_1359() {
      expectParserSuccess("from Animal a where a.description = :nonstring");
   }

   @Test
   public void testASTParserLoadingTestMultipleBagFetchesFail_1361() {
      expectParserSuccess("from Human h join fetch h.friends f join fetch f.friends fof");
   }

   @Test
   public void testASTParserLoadingTestCollectionFetchWithDistinctionAndLimit_1366() {
      expectParserSuccess("select distinct p from Animal p inner join fetch p.offspring");
   }

   @Test
   public void testASTParserLoadingTestCollectionFetchWithDistinctionAndLimit_1367() {
      expectParserSuccess("select p from Animal p inner join fetch p.offspring order by p.id");
   }

   @Test
   public void testASTParserLoadingTestQueryMetadataRetrievalWithFetching_1371() {
      expectParserSuccess("from Animal a inner join fetch a.mother");
   }

   @Test
   public void testASTParserLoadingTestSuperclassPropertyReferenceAfterCollectionIndexedAccess_1373() {
      expectParserSuccess("from Zoo zoo where zoo.mammals['tiger'].mother.bodyWeight > 3.0f");
   }

   @Test
   public void testASTParserLoadingTestJoinFetchCollectionOfValues_1375() {
      expectParserSuccess("select h from Human as h join fetch h.nickNames");
   }

   @Test
   public void testASTParserLoadingTestIntegerLiterals_1377() {
      expectParserSuccess("from Foo where long = 1");
   }

   @Test
   public void testASTParserLoadingTestIntegerLiterals_1378() {
      expectParserSuccess("from Foo where long = 1L");
   }

   @Test
   public void testASTParserLoadingTestDecimalLiterals_1380() {
      expectParserSuccess("from Animal where bodyWeight > 100.0e-10");
   }

   @Test
   public void testASTParserLoadingTestDecimalLiterals_1381() {
      expectParserSuccess("from Animal where bodyWeight > 100.0E-10");
   }

   @Test
   public void testASTParserLoadingTestDecimalLiterals_1382() {
      expectParserSuccess("from Animal where bodyWeight > 100.001f");
   }

   @Test
   public void testASTParserLoadingTestDecimalLiterals_1383() {
      expectParserSuccess("from Animal where bodyWeight > 100.001F");
   }

   @Test
   public void testASTParserLoadingTestDecimalLiterals_1384() {
      expectParserSuccess("from Animal where bodyWeight > 100.001d");
   }

   @Test
   public void testASTParserLoadingTestDecimalLiterals_1385() {
      expectParserSuccess("from Animal where bodyWeight > 100.001D");
   }

   @Test
   public void testASTParserLoadingTestDecimalLiterals_1386() {
      expectParserSuccess("from Animal where bodyWeight > .001f");
   }

   @Test
   public void testASTParserLoadingTestDecimalLiterals_1387() {
      expectParserSuccess("from Animal where bodyWeight > 100e-10");
   }

   @Test
   public void testASTParserLoadingTestDecimalLiterals_1388() {
      expectParserSuccess("from Animal where bodyWeight > .01E-10");
   }

   @Test
   public void testASTParserLoadingTestDecimalLiterals_1389() {
      expectParserSuccess("from Animal where bodyWeight > 1e-38");
   }

   @Test
   public void testASTParserLoadingTestNakedPropertyRef_1391() {
      expectParserSuccess("from Animal where bodyWeight = bodyWeight");
   }

   @Test
   public void testASTParserLoadingTestNakedPropertyRef_1392() {
      expectParserSuccess("select bodyWeight from Animal");
   }

   @Test
   public void testASTParserLoadingTestNakedPropertyRef_1393() {
      expectParserSuccess("select max(bodyWeight) from Animal");
   }

   @Test
   public void testASTParserLoadingTestNakedComponentPropertyRef_1395() {
      expectParserSuccess("select name from Human");
   }

   @Test
   public void testASTParserLoadingTestNakedImplicitJoins_1399() {
      expectParserSuccess("from Animal where mother.father.id = 1");
   }

   @Test
   public void testASTParserLoadingTestNakedEntityAssociationReference_1401() {
      expectParserSuccess("from Animal where mother = :mother");
   }

   @Test
   public void testASTParserLoadingTestNakedMapIndex_1403() {
      expectParserSuccess("from Zoo where mammals['dog'].description like '%black%'");
   }

   @Test
   public void testASTParserLoadingTestInvalidFetchSemantics_1405() {
      expectParserSuccess("select mother from Human a left join fetch a.mother mother");
   }

   @Test
   public void testASTParserLoadingTestArithmetic_1407() {
      expectParserSuccess("select 2 from Zoo");
   }

   @Test
   public void testASTParserLoadingTestNestedCollectionFetch_1417() {
      expectParserSuccess("from Animal a left join fetch a.offspring o left join fetch o.offspring where a.mother.id = 1 order by a.description");
   }

   @Test
   public void testASTParserLoadingTestNestedCollectionFetch_1418() {
      expectParserSuccess("from Zoo z left join fetch z.animals a left join fetch a.offspring where z.name ='MZ' order by a.description");
   }

   @Test
   public void testASTParserLoadingTestNestedCollectionFetch_1419() {
      expectParserSuccess("from Human h left join fetch h.pets a left join fetch a.offspring where h.name.first ='Gavin' order by a.description");
   }

   @Test
   public void testASTParserLoadingTestInitProxy_1424() {
      expectParserSuccess("from Animal a");
   }

   @Test
   public void testASTParserLoadingTestSelectClauseImplicitJoin_1426() {
      expectParserSuccess("select distinct a.zoo from Animal a where a.zoo is not null");
   }

   @Test
   public void testASTParserLoadingTestComponentOrderBy_1428() {
      expectParserSuccess("from Human as h order by h.name");
   }

   @Test
   public void testASTParserLoadingTestAliases_1433() {
      expectParserSuccess("select a.bodyWeight as abw, a.description from Animal a");
   }

   @Test
   public void testASTParserLoadingTestAliases_1434() {
      expectParserSuccess("select count(*), avg(a.bodyWeight) as avg from Animal a");
   }

   @Test
   public void testASTParserLoadingTestParameterMixing_1436() {
      expectParserSuccess("from Animal a where a.description = ? and a.bodyWeight = ? or a.bodyWeight = :bw");
   }

   @Test
   public void testASTParserLoadingTestOrdinalParameters_1438() {
      expectParserSuccess("from Animal a where a.description = :description and a.bodyWeight = :weight");
   }

   @Test
   public void testASTParserLoadingTestOrdinalParameters_1439() {
      expectParserSuccess("from Animal a where a.bodyWeight in (?, ?)");
   }

   @Test
   public void testASTParserLoadingTestIndexParams_1441() {
      expectParserSuccess("from Zoo zoo where zoo.mammals[:name] = :id");
   }

   @Test
   public void testASTParserLoadingTestIndexParams_1442() {
      expectParserSuccess("from Zoo zoo where zoo.mammals[:name].bodyWeight > :w");
   }

   @Test
   public void testASTParserLoadingTestIndexParams_1443() {
      expectParserSuccess("from Zoo zoo where zoo.animals[:sn].mother.bodyWeight < :mw");
   }

   @Test
   public void testASTParserLoadingTestIndexParams_1444() {
      expectParserSuccess("from Zoo zoo where zoo.animals[:sn].description like :desc and zoo.animals[:sn].bodyWeight > :wmin and zoo.animals[:sn].bodyWeight < :wmax");
   }

   @Test
   public void testASTParserLoadingTestIndexParams_1445() {
      expectParserSuccess("from Human where addresses[:type].city = :city and addresses[:type].country = :country");
   }

   @Test
   public void testASTParserLoadingTestAggregation_1447() {
      expectParserSuccess("select sum(h.bodyWeight) from Human h");
   }

   @Test
   public void testASTParserLoadingTestAggregation_1448() {
      expectParserSuccess("select avg(h.height) from Human h");
   }

   @Test
   public void testASTParserLoadingTestAggregation_1449() {
      expectParserSuccess("select max(a.id) from Animal a");
   }

   @Test
   public void testASTParserLoadingTestImplicitPolymorphism_1454() {
      expectParserSuccess("from java.lang.Comparable");
   }

   @Test
   public void testASTParserLoadingTestImplicitPolymorphism_1455() {
      expectParserSuccess("from java.lang.Object");
   }

   @Test
   public void testASTParserLoadingTestCoalesce_1457() {
      expectParserSuccess("from Human h where coalesce(h.nickName, h.name.first, h.name.last) = 'max'");
   }

   @Test
   public void testASTParserLoadingTestCoalesce_1458() {
      expectParserSuccess("select nullif(nickName, '1e1') from Human");
   }

   @Test
   public void testASTParserLoadingTestStr_1460() {
      expectParserSuccess("select str(an.bodyWeight) from Animal an where str(an.bodyWeight) like '%1%'");
   }

   @Test
   public void testASTParserLoadingTestStr_1461() {
      expectParserSuccess("select str(an.bodyWeight, 8, 3) from Animal an where str(an.bodyWeight, 8, 3) like '%1%'");
   }

   @Test
   public void testASTParserLoadingTestCast_1465() {
      expectParserSuccess("from Human h where h.nickName like 'G%'");
   }

   @Test
   public void testASTParserLoadingTestSelectExpressions_1477() {
      expectParserSuccess("select a.bodyWeight from Animal a join a.mother m");
   }

   @Test
   public void testASTParserLoadingTestSelectExpressions_1479() {
      expectParserSuccess("select sum(a.bodyWeight) from Animal a join a.mother m");
   }

   @Test
   public void testASTParserLoadingTestSelectExpressions_1481() {
      expectParserSuccess("select concat(h.name.first, ' ', h.name.initial, ' ', h.name.last) from Human h");
   }

   @Test
   public void testASTParserLoadingTestSelectExpressions_1483() {
      expectParserSuccess("select nickName from Human");
   }

   @Test
   public void testASTParserLoadingTestSelectExpressions_1485() {
      expectParserSuccess("select abs(bodyWeight) from Human");
   }

   @Test
   public void testASTParserLoadingTestSelectExpressions_1491() {
      expectParserSuccess("select sum(abs(bodyWeight)) from Animal");
   }

   @Test
   public void testASTParserLoadingTestImplicitJoin_1493() {
      expectParserSuccess("from Animal a where a.mother.bodyWeight < 2.0 or a.mother.bodyWeight > 9.0");
   }

   @Test
   public void testASTParserLoadingTestImplicitJoin_1494() {
      expectParserSuccess("from Animal a where a.mother.bodyWeight > 2.0 and a.mother.bodyWeight > 9.0");
   }

   @Test
   public void testASTParserLoadingTestSimpleSelect_1496() {
      expectParserSuccess("select a.mother from Animal as a");
   }

   @Test
   public void testASTParserLoadingTestWhere_1498() {
      expectParserSuccess("from Animal an where an.bodyWeight > 10");
   }

   @Test
   public void testASTParserLoadingTestWhere_1499() {
      expectParserSuccess("from Animal an where not an.bodyWeight > 10");
   }

   @Test
   public void testASTParserLoadingTestWhere_1500() {
      expectParserSuccess("from Animal an where an.bodyWeight between 0 and 10");
   }

   @Test
   public void testASTParserLoadingTestWhere_1501() {
      expectParserSuccess("from Animal an where an.bodyWeight not between 0 and 10");
   }

   @Test
   public void testASTParserLoadingTestWhere_1503() {
      expectParserSuccess("from Animal an where (an.bodyWeight > 10 and an.bodyWeight < 100) or an.bodyWeight is null",
            "(QUERY (QUERY_SPEC (SELECT_FROM (from (PERSISTER_SPACE (ENTITY_PERSISTER_REF Animal an))) (SELECT (SELECT_LIST (SELECT_ITEM an)))) (where (or (and (> (PATH (. an bodyWeight)) 10) (< (PATH (. an bodyWeight)) 100)) (is null (PATH (. an bodyWeight)))))))");
   }

   @Test
   public void testASTParserLoadingTestEntityFetching_1505() {
      expectParserSuccess("from Animal an join fetch an.mother");
   }

   @Test
   public void testASTParserLoadingTestEntityFetching_1506() {
      expectParserSuccess("select an from Animal an join fetch an.mother");
   }

   @Test
   public void testASTParserLoadingTestCollectionFetching_1508() {
      expectParserSuccess("from Animal an join fetch an.offspring");
   }

   @Test
   public void testASTParserLoadingTestCollectionFetching_1509() {
      expectParserSuccess("select an from Animal an join fetch an.offspring");
   }

   @Test
   public void testASTParserLoadingTestProjectionQueries_1511() {
      expectParserSuccess("select an.mother.id, max(an.bodyWeight) from Animal an group by an.mother.id");
   }

   @Test
   public void testASTParserLoadingTestResultTransformerScalarQueries_1519() {
      expectParserSuccess("select an.description as description, an.bodyWeight as bodyWeight from Animal an order by bodyWeight desc");
   }

   @Test
   public void testASTParserLoadingTestResultTransformerScalarQueries_1520() {
      expectParserSuccess("select a from Animal a, Animal b order by a.id");
   }

   @Test
   public void testASTParserLoadingTestResultTransformerEntityQueries_1522() {
      expectParserSuccess("select an as an from Animal an order by bodyWeight desc");
   }

   @Test
   public void testASTParserLoadingTestEJBQLFunctions_1524() {
      expectParserSuccess("from Animal a where a.description = concat('1', concat('2','3'), '45')");
   }

   @Test
   public void testASTParserLoadingTestEJBQLFunctions_1525() {
      expectParserSuccess("from Animal a where substring(a.description, 1, 3) = 'cat'");
   }

   @Test
   public void testASTParserLoadingTestEJBQLFunctions_1529() {
      expectParserSuccess("from Animal a where length(a.description) = 5");
   }

   @Test
   public void testASTParserLoadingTestEJBQLFunctions_1530() {
      expectParserSuccess("from Animal a where locate('abc', a.description, 2) = 2");
   }

   @Test
   public void testASTParserLoadingTestEJBQLFunctions_1531() {
      expectParserSuccess("from Animal a where locate('abc', a.description) = 2");
   }

   @Test
   public void testASTParserLoadingTestEJBQLFunctions_1532() {
      expectParserSuccess("select locate('cat', a.description, 2) from Animal a");
   }

   @Test
   public void testASTParserLoadingTestEJBQLFunctions_1541() {
      expectParserSuccess("from Animal a where a.description like '%a%'");
   }

   @Test
   public void testASTParserLoadingTestEJBQLFunctions_1542() {
      expectParserSuccess("from Animal a where a.description not like '%a%'");
   }

   @Test
   public void testASTParserLoadingTestEJBQLFunctions_1543() {
      expectParserSuccess("from Animal a where a.description like 'x%ax%' escape 'x'");
   }

   @Test
   public void testASTParserLoadingTestSubselectBetween_1546() {
      expectParserSuccess("from Animal x where (x.name, x.bodyWeight) = ('cat', 20)");
   }

   @Test
   public void testHQLPARSER_71_1548() {
      expectParserSuccess("select p from pppoe_test p where p.sourceIP=:source_ip and p.login>:logentrytime and (p.logout>:logentrytime OR p.logout IS NULL)");
   }

   @Test
   public void testHQLPARSER_71_1549() {
      expectParserSuccess("select a from Animal a where TrUE");
   }

   @Test
   public void testHQLPARSER_71_1550() {
      expectParserSuccess("select a from eg.Animal a where FALSE");
   }
}
