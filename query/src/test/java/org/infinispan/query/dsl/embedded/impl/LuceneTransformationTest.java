package org.infinispan.query.dsl.embedded.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.hibernate.search.engine.integration.impl.ExtendedSearchIntegrator;
import org.hibernate.search.exception.SearchException;
import org.hibernate.search.testsupport.junit.SearchFactoryHolder;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParser;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionEntityNamesResolver;
import org.infinispan.query.dsl.embedded.impl.model.Employee;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test the parsing and transformation of Ickle queries to Lucene queries.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public class LuceneTransformationTest {

   @Rule
   public SearchFactoryHolder factoryHolder = new SearchFactoryHolder(Employee.class);

   @Rule
   public ExpectedException expectedException = ExpectedException.none();

   @Test
   public void testRaiseExceptionDueToUnknownAlias() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028502: Unknown alias: a.");

      parseAndTransform("from org.infinispan.query.dsl.embedded.impl.model.Employee e where a.name = 'same'");
   }

   @Test
   public void testMatchAllQuery() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee",
            "*:*");

      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e",
            "*:*");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e",
            "*:*");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where true",
            "*:*");
   }

   @Test
   public void testRejectAllQuery() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where false",
            "-*:* #*:*");   //TODO [anistor] maybe this should be "-*:*"
   }

   @Test
   public void testFullTextKeyword() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : 'bicycle'",
            "text:bicycle");
   }

   @Test
   public void testFullTextWildcard() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : 'geo*e'",
            "text:geo*e");
   }

   @Test
   public void testFullTextFuzzy() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : 'jonh'~2",
            "text:jonh~2");
   }

   @Test
   public void testFullTextPhrase() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : 'twisted cables'",
            "text:\"twisted cables\"");
   }

   @Test
   public void testFullTextRegexp() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : /te?t/",
            "text:/te?t/");
   }

   @Test
   public void testFullTextRange() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : ['AAA' 'ZZZ']",
            "text:[aaa TO zzz]");

      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : ['AAA' to 'ZZZ']",
            "text:[aaa TO zzz]");

      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : [* to 'eeee']",
            "text:[* TO eeee]");

      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : ['eeee' to *]",
            "text:[eeee TO *]");

      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : [* *]",
            "text:[* TO *]");
   }

   @Test
   public void testFullTextFieldBoost() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : 'foo'^3.0'",
            "text:foo^3.0");

      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : ('foo')^3.0'",
            "text:foo^3.0");

      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : ('foo' and 'bar')^3.0'",
            "(+text:foo +text:bar)^3.0");

      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : 'foo'^3.0 || e.analyzedInfo : 'bar'",
            "text:foo^3.0 analyzedInfo:bar");
   }

   @Test
   public void testFullTextFieldOccur() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : (-'foo') ",
            "-text:foo #*:*");

      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : (-'foo' +'bar')",
            "(-text:foo) (+text:bar)");

      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where not e.text : (-'foo' +'bar')",
            "-((-text:foo) (+text:bar)) #*:*");

// TODO [anistor] fix this
//      assertGeneratedLuceneQuery(
//            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where !e.text : (-'foo' +'bar')",
//            "-((-text:foo) (+text:bar)) #*:*");

      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text : (-'foo' '*')",
            "(-text:foo) text:*");
   }

   @Test
   public void testRestrictedQueryUsingSelect() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name = 'same' or e.id = 5",
            "name:same id:5");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name = 'same' and e.id = 5",
            "+name:same +id:5");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name = 'same' and not e.id = 5",
            "+name:same -id:5");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name = 'same' or not e.id = 5",
            "name:same (-id:5 #*:*)");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where not e.id = 5",
            "-id:5 #*:*");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.id != 5",
            "-id:5 #*:*");
   }

   @Test
   public void testFieldMapping() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.someMoreInfo = 'foo'",
            "someMoreInfo:foo");

      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.sameInfo = 'foo'",
            "sameInfo:foo");
   }

   @Test
   public void testWrongFieldName() {
      expectedException.expect(SearchException.class);
      expectedException.expectMessage("Unable to find field otherInfo in org.infinispan.query.dsl.embedded.impl.model.Employee");

      parseAndTransform("from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.otherInfo = 'foo'");
   }

   @Test
   public void testProjectionQuery() {
      LuceneQueryParsingResult<Class<?>> parsingResult = parseAndTransform("select e.id, e.name from org.infinispan.query.dsl.embedded.impl.model.Employee e");

      assertThat(parsingResult.getQuery().toString()).isEqualTo("*:*");
      assertThat(parsingResult.getProjections()).isEqualTo(new String[]{"id", "name"});
   }

   @Test
   public void testEmbeddedProjectionQuery() {
      LuceneQueryParsingResult<Class<?>> parsingResult = parseAndTransform("select e.author.name from org.infinispan.query.dsl.embedded.impl.model.Employee e");

      assertThat(parsingResult.getQuery().toString()).isEqualTo("*:*");
      assertThat(parsingResult.getProjections()).isEqualTo(new String[]{"author.name"});
   }

   @Test
   public void testNestedEmbeddedProjectionQuery() {
      LuceneQueryParsingResult<Class<?>> parsingResult = parseAndTransform("select e.author.address.street from org.infinispan.query.dsl.embedded.impl.model.Employee e");

      assertThat(parsingResult.getQuery().toString()).isEqualTo("*:*");
      assertThat(parsingResult.getProjections()).isEqualTo(new String[]{"author.address.street"});
   }

   @Test
   public void testQueryWithUnqualifiedPropertyReferences() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where name = 'same' and not id = 5",
            "+name:same -id:5");
   }

   @Test
   public void testNegatedQuery() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where NOT e.name = 'same'",
            "-name:same #*:*");

      // JPQL syntax
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name <> 'same'",
            "-name:same #*:*");

      // HQL syntax
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name != 'same'",
            "-name:same #*:*");
   }

   @Test
   public void testNegatedQueryOnNumericProperty() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.position <> 3",
            "-position:[3 TO 3] #*:*");
   }

   @Test
   public void testNegatedRangeQuery() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee where name = 'Bob' and not position between 1 and 3",
            "+name:Bob -position:[1 TO 3]");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name = 'Bob' and not e.position between 1 and 3",
            "+name:Bob -position:[1 TO 3]");
   }

   @Test
   public void testQueryWithNamedParameter() {
      Map<String, Object> namedParameters = new HashMap<>();
      namedParameters.put("nameParameter", "Bob");

      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee where name = :nameParameter",
            namedParameters,
            "name:Bob");

      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name = :nameParameter",
            namedParameters,
            "name:Bob");
   }

   @Test
   public void testBooleanQuery() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name = 'same' or (e.id = 4 and e.name = 'Bob')",
            "name:same (+id:4 +name:Bob)");
   }

   @Test
   public void testBooleanQueryUsingSelect() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name = 'same' or (e.id = 4 and e.name = 'Bob')",
            "name:same (+id:4 +name:Bob)");
   }

   @Test
   public void testBetweenQuery() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name between 'aaa' and 'zzz'",
            "name:[aaa TO zzz]");
   }

   @Test
   public void testNotBetweenQuery() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name not between 'aaa' and 'zzz'",
            "-name:[aaa TO zzz] #*:*");
   }

   @Test
   public void testNumericNotBetweenQuery() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where not e.position between 1 and 3",
            "-position:[1 TO 3] #*:*");
   }

   @Test
   public void testBetweenQueryForCharacterLiterals() {
      assertGeneratedLuceneQuery("select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name between 'a' and 'z'", "name:[a TO z]");
   }

   @Test
   public void testBetweenQueryWithNamedParameters() {
      Map<String, Object> namedParameters = new HashMap<>();
      namedParameters.put("p1", "aaa");
      namedParameters.put("p2", "zzz");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name between :p1 and :p2",
            namedParameters,
            "name:[aaa TO zzz]");
   }

   @Test
   public void testNumericBetweenQuery() {
      Map<String, Object> namedParameters = new HashMap<>();
      namedParameters.put("p1", 10L);
      namedParameters.put("p2", 20L);

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.position between :p1 and :p2",
            namedParameters,
            "position:[10 TO 20]");
   }

   @Test
   public void testQueryWithEmbeddedPropertyInFromClause() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.author.name = 'Bob'",
            "author.name:Bob");
   }

   @Test
   public void testLessThanQuery() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.position < 100",
            "position:[* TO 100}");
   }

   @Test
   public void testLessThanOrEqualsToQuery() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.position <= 100",
            "position:[* TO 100]");
   }

   @Test
   public void testGreaterThanOrEqualsToQuery() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee where position >= 100",
            "position:[100 TO *]");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.position >= 100",
            "position:[100 TO *]");
   }

   @Test
   public void testGreaterThanQuery() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee where position > 100",
            "position:{100 TO *]");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.position > 100",
            "position:{100 TO *]");
   }

   @Test
   public void testInQuery() {
      assertGeneratedLuceneQuery(
            "from org.infinispan.query.dsl.embedded.impl.model.Employee where name in ('Bob', 'Alice')",
            "name:Bob name:Alice");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name in ('Bob', 'Alice')",
            "name:Bob name:Alice");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.position in (10, 20, 30, 40)",
            "position:[10 TO 10] position:[20 TO 20] position:[30 TO 30] position:[40 TO 40]");
   }

   @Test
   public void testInQueryWithNamedParameters() {
      Map<String, Object> namedParameters = new HashMap<>();
      namedParameters.put("name1", "Bob");
      namedParameters.put("name2", "Alice");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name in (:name1, :name2)",
            namedParameters,
            "name:Bob name:Alice");

      namedParameters = new HashMap<>();
      namedParameters.put("pos1", 10);
      namedParameters.put("pos2", 20);
      namedParameters.put("pos3", 30);
      namedParameters.put("pos4", 40);

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.position in (:pos1, :pos2, :pos3, :pos4)",
            namedParameters,
            "position:[10 TO 10] position:[20 TO 20] position:[30 TO 30] position:[40 TO 40]");
   }

   @Test
   public void testNotInQuery() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name not in ('Bob', 'Alice')",
            "-(name:Bob name:Alice) #*:*");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.position not in (10, 20, 30, 40)",
            "-(position:[10 TO 10] position:[20 TO 20] position:[30 TO 30] position:[40 TO 40]) #*:*");
   }

   @Test
   public void testLikeQuery() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name LIKE 'Al_ce'",
            "name:Al?ce");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name LIKE 'Ali%'",
            "name:Ali*");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name LIKE 'Ali%%'",
            "name:Ali**");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name LIKE '_l_ce'",
            "name:?l?ce");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name LIKE '___ce'",
            "name:???ce");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name LIKE '_%_ce'",
            "name:?*?ce");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name LIKE 'Alice in wonderl%'",
            "name:Alice in wonderl*");
   }

   @Test
   public void testNotLikeQuery() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name NOT LIKE 'Al_ce'",
            "-name:Al?ce #*:*");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name NOT LIKE 'Ali%'",
            "-name:Ali* #*:*");

      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name NOT LIKE '_l_ce' and not (e.title LIKE '%goo' and e.position = '5')",
            "-name:?l?ce -(+title:*goo +position:[5 TO 5]) #*:*");
   }

   @Test
   public void testIsNullQuery() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name IS null",
            "name:_null_");
   }

   @Test
   public void testIsNullQueryForEmbeddedEntity() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.author IS null",
            "author:_null_");
   }

   @Test
   public void testIsNotNullQuery() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name IS NOT null",
            "-name:_null_ #*:*");
   }

   @Test
   public void testCollectionOfEmbeddableQuery() {
      assertGeneratedLuceneQuery(
            "select e from org.infinispan.query.dsl.embedded.impl.model.Employee e JOIN e.contactDetails d WHERE d.email = 'ninja647@mailinator.com' ",
            "contactDetails.email:ninja647@mailinator.com");
   }

   @Test
   public void testCollectionOfEmbeddableInEmbeddedQuery() {
      assertGeneratedLuceneQuery(
            "SELECT e FROM org.infinispan.query.dsl.embedded.impl.model.Employee e "
                  + " JOIN e.contactDetails d"
                  + " JOIN d.address.alternatives as a "
                  + "WHERE a.postCode = '90210' ",
            "contactDetails.address.alternatives.postCode:90210");
   }

   @Test
   public void testRaiseExceptionDueToUnknownQualifiedProperty() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028501: The type org.infinispan.query.dsl.embedded.impl.model.Employee has no property named 'foobar'.");

      parseAndTransform("from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.foobar = 'same'");
   }

   @Test
   public void testRaiseExceptionDueToUnknownUnqualifiedProperty() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028501: The type org.infinispan.query.dsl.embedded.impl.model.Employee has no property named 'foobar'.");

      parseAndTransform("from org.infinispan.query.dsl.embedded.impl.model.Employee e where foobar = 'same'");
   }

   @Test
   public void testRaiseExceptionDueToAnalyzedPropertyInFromClause() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028522: No relational queries can be applied to property 'text' in type org.infinispan.query.dsl.embedded.impl.model.Employee since the property is analyzed.");

      parseAndTransform("from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.text = 'foo'");
   }

   @Test
   public void testRaiseExceptionDueToUnknownPropertyInSelectClause() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028501: The type org.infinispan.query.dsl.embedded.impl.model.Employee has no property named 'foobar'.");

      parseAndTransform("select e.foobar from org.infinispan.query.dsl.embedded.impl.model.Employee e");
   }

   @Test
   public void testRaiseExceptionDueToUnknownPropertyInEmbeddedSelectClause() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028501: The type org.infinispan.query.dsl.embedded.impl.model.Employee has no property named 'foo'.");

      parseAndTransform("select e.author.foo from org.infinispan.query.dsl.embedded.impl.model.Employee e");
   }

   @Test
   public void testRaiseExceptionDueToSelectionOfCompleteEmbeddedEntity() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028503: Property author can not be selected from type org.infinispan.query.dsl.embedded.impl.model.Employee since it is an embedded entity.");

      parseAndTransform("select e.author from org.infinispan.query.dsl.embedded.impl.model.Employee e");
   }

   @Test
   public void testRaiseExceptionDueToUnqualifiedSelectionOfCompleteEmbeddedEntity() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028503: Property author can not be selected from type org.infinispan.query.dsl.embedded.impl.model.Employee since it is an embedded entity.");

      parseAndTransform("select author from org.infinispan.query.dsl.embedded.impl.model.Employee e");
   }

   @Test
   public void testDetermineTargetEntityType() {
      LuceneQueryParsingResult<Class<?>> result = parseAndTransform("select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name = 'same' and not e.id = 5");
      assertThat(result.getTargetEntityMetadata()).isSameAs(Employee.class);
      assertThat(result.getTargetEntityName()).isEqualTo("org.infinispan.query.dsl.embedded.impl.model.Employee");

      result = parseAndTransform("select e from org.infinispan.query.dsl.embedded.impl.model.Employee e");
      assertThat(result.getTargetEntityMetadata()).isSameAs(Employee.class);
      assertThat(result.getTargetEntityName()).isEqualTo("org.infinispan.query.dsl.embedded.impl.model.Employee");
   }

   @Test
   public void testBuildOneFieldSort() {
      LuceneQueryParsingResult<Class<?>> result = parseAndTransform("select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name = 'same' order by e.title");
      Sort sort = result.getSort();
      assertThat(sort).isNotNull();
      assertThat(sort.getSort().length).isEqualTo(1);
      assertThat(sort.getSort()[0].getField()).isEqualTo("title");
      assertThat(sort.getSort()[0].getReverse()).isEqualTo(false);
      assertThat(sort.getSort()[0].getType()).isEqualTo(SortField.Type.STRING);
   }

   @Test
   public void testBuildTwoFieldsSort() {
      LuceneQueryParsingResult<Class<?>> result = parseAndTransform("select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name = 'same' order by e.title, e.position DESC");
      Sort sort = result.getSort();
      assertThat(sort).isNotNull();
      assertThat(sort.getSort().length).isEqualTo(2);
      assertThat(sort.getSort()[0].getField()).isEqualTo("title");
      assertThat(sort.getSort()[0].getReverse()).isEqualTo(false);
      assertThat(sort.getSort()[0].getType()).isEqualTo(SortField.Type.STRING);
      assertThat(sort.getSort()[1].getField()).isEqualTo("position");
      assertThat(sort.getSort()[1].getReverse()).isEqualTo(true);
      assertThat(sort.getSort()[1].getType()).isEqualTo(SortField.Type.LONG);
   }

   @Test
   public void testBuildSortForNullEncoding() {
      LuceneQueryParsingResult<Class<?>> result = parseAndTransform("select e from org.infinispan.query.dsl.embedded.impl.model.Employee e order by e.code DESC");
      Sort sort = result.getSort();
      assertThat(sort).isNotNull();
      assertThat(sort.getSort().length).isEqualTo(1);
      assertThat(sort.getSort()[0].getField()).isEqualTo("code");
      assertThat(sort.getSort()[0].getType()).isEqualTo(SortField.Type.LONG);
   }

   @Test
   public void testRaiseExceptionDueToUnrecognizedSortDirection() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028526: Invalid query: select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name = 'same' order by e.title DESblah, e.name ASC;");

      parseAndTransform("select e from org.infinispan.query.dsl.embedded.impl.model.Employee e where e.name = 'same' order by e.title DESblah, e.name ASC");
   }

   @Test
   public void testBeAbleToJoinOnCollectionOfEmbedded() {
      LuceneQueryParsingResult<Class<?>> result = parseAndTransform("select d.email from org.infinispan.query.dsl.embedded.impl.model.Employee e JOIN e.contactDetails d");

      assertThat(result.getQuery().toString()).isEqualTo("*:*");
      assertThat(result.getProjections()).containsOnly("contactDetails.email");
   }

   @Test
   public void testBeAbleToJoinOnCollectionOfEmbeddedWithEmbedded() {
      LuceneQueryParsingResult<Class<?>> result = parseAndTransform("select d.email from org.infinispan.query.dsl.embedded.impl.model.Employee e JOIN e.contactDetails d WHERE d.address.postCode='EA123'");

      assertThat(result.getQuery().toString()).isEqualTo("contactDetails.address.postCode:EA123");
      assertThat(result.getProjections()).containsOnly("contactDetails.email");
   }

   @Test
   public void testBeAbleToJoinOnCollectionOfEmbeddedWithEmbeddedAndUseInOperator() {
      LuceneQueryParsingResult<Class<?>> result = parseAndTransform("select d.email from org.infinispan.query.dsl.embedded.impl.model.Employee e JOIN e.contactDetails d WHERE d.address.postCode IN ('EA123')");

      assertThat(result.getQuery().toString()).isEqualTo("contactDetails.address.postCode:EA123");
      assertThat(result.getProjections()).containsOnly("contactDetails.email");
   }

   @Test
   public void testBeAbleToJoinOnCollectionOfEmbeddedWithEmbeddedAndUseBetweenOperator() {
      LuceneQueryParsingResult<Class<?>> result = parseAndTransform("select d.email from org.infinispan.query.dsl.embedded.impl.model.Employee e JOIN e.contactDetails d WHERE d.address.postCode BETWEEN '0000' AND 'ZZZZ'");

      assertThat(result.getQuery().toString()).isEqualTo("contactDetails.address.postCode:[0000 TO ZZZZ]");
      assertThat(result.getProjections()).containsOnly("contactDetails.email");
   }

   @Test
   public void testBeAbleToJoinOnCollectionOfEmbeddedWithEmbeddedAndUseGreaterOperator() {
      LuceneQueryParsingResult<Class<?>> result = parseAndTransform("select d.email from org.infinispan.query.dsl.embedded.impl.model.Employee e JOIN e.contactDetails d WHERE d.address.postCode > '0000'");

      assertThat(result.getQuery().toString()).isEqualTo("contactDetails.address.postCode:{0000 TO *]");
      assertThat(result.getProjections()).containsOnly("contactDetails.email");
   }

   @Test
   public void testBeAbleToJoinOnCollectionOfEmbeddedWithEmbeddedAndUseLikeOperator() {
      LuceneQueryParsingResult<Class<?>> parsingResult = parseAndTransform("select d.email from org.infinispan.query.dsl.embedded.impl.model.Employee e JOIN e.contactDetails d WHERE d.address.postCode LIKE 'EA1%'");

      assertThat(parsingResult.getQuery().toString()).isEqualTo("contactDetails.address.postCode:EA1*");
      assertThat(parsingResult.getProjections()).containsOnly("contactDetails.email");
   }

   @Test
   public void testBeAbleToProjectUnqualifiedField() {
      LuceneQueryParsingResult<Class<?>> result = parseAndTransform("SELECT name, text FROM org.infinispan.query.dsl.embedded.impl.model.Employee e JOIN e.contactDetails d");

      assertThat(result.getQuery().toString()).isEqualTo("*:*");
      assertThat(result.getProjections()).containsOnly("name", "text");
   }

   @Test
   public void testBeAbleToProjectUnqualifiedFieldAndQualifiedField() {
      LuceneQueryParsingResult<Class<?>> result = parseAndTransform("SELECT name, text, d.email FROM org.infinispan.query.dsl.embedded.impl.model.Employee e JOIN e.contactDetails d");

      assertThat(result.getQuery().toString()).isEqualTo("*:*");
      assertThat(result.getProjections()).containsOnly("name", "text", "contactDetails.email");
   }

   @Test
   public void testBeAbleToProjectQualifiedField() {
      LuceneQueryParsingResult<Class<?>> result = parseAndTransform("SELECT e.name, e.text, d.email FROM org.infinispan.query.dsl.embedded.impl.model.Employee e JOIN e.contactDetails d");

      assertThat(result.getQuery().toString()).isEqualTo("*:*");
      assertThat(result.getProjections()).containsOnly("name", "text", "contactDetails.email");
   }

   @Test
   public void testBeAbleToJoinOnCollectionOfEmbeddedWithTwoEmbeddedCollections() {
      LuceneQueryParsingResult<Class<?>> result = parseAndTransform(
            " SELECT d.email " +
                  " FROM org.infinispan.query.dsl.embedded.impl.model.Employee e " +
                  " JOIN e.contactDetails d " +
                  " JOIN e.alternativeContactDetails a" +
                  " WHERE d.address.postCode='EA123' AND a.email='ninja647@mailinator.com'");

      assertThat(result.getQuery().toString()).isEqualTo("+contactDetails.address.postCode:EA123 +alternativeContactDetails.email:ninja647@mailinator.com");
      assertThat(result.getProjections()).containsOnly("contactDetails.email");
   }

   private void assertGeneratedLuceneQuery(String queryString, String expectedLuceneQuery) {
      assertGeneratedLuceneQuery(queryString, null, expectedLuceneQuery);
   }

   private void assertGeneratedLuceneQuery(String queryString, Map<String, Object> namedParameters, String expectedLuceneQuery) {
      LuceneQueryParsingResult<Class<?>> result = parseAndTransform(queryString, namedParameters);
      assertThat(result.getQuery().toString()).isEqualTo(expectedLuceneQuery);
   }

   private LuceneQueryParsingResult<Class<?>> parseAndTransform(String queryString) {
      return parseAndTransform(queryString, null);
   }

   private LuceneQueryParsingResult<Class<?>> parseAndTransform(String queryString, Map<String, Object> namedParameters) {
      ExtendedSearchIntegrator searchFactory = factoryHolder.getSearchFactory();
      HibernateSearchPropertyHelper propertyHelper = new HibernateSearchPropertyHelper(searchFactory,
            new ReflectionEntityNamesResolver(null), LuceneTransformationTest.class.getClassLoader());

      IckleParsingResult<Class<?>> ickleParsingResult = IckleParser.parse(queryString, propertyHelper);

      LuceneQueryMaker<Class<?>> luceneQueryMaker = new LuceneQueryMaker<>(searchFactory, propertyHelper.getDefaultFieldBridgeProvider());
      return luceneQueryMaker.transform(ickleParsingResult, namedParameters, ickleParsingResult.getTargetEntityMetadata());
   }
}
