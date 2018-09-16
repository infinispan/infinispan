package org.infinispan.query.analysis;

import static java.util.Arrays.asList;
import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Copied and adapted from Hibernate Search
 * org.hibernate.search.test.analyzer.solr.SolrAnalyzerTest
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 * @author Emmanuel Bernard
 * @author Hardy Ferentschik
 */
@Test(groups = "functional", testName = "query.analysis.AnalyzerTest")
public class AnalyzerTest extends SingleCacheManagerTest {

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
            .index(Index.ALL)
            .addIndexedEntity(Team.class)
            .addProperty("hibernate.search.default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   /**
    * Tests that the token filters applied to <code>Team</code> are successfully created and used. Refer to
    * <code>Team</code> to see the exact definitions.
    *
    * @throws Exception in case the test fails
    */
   public void testAnalyzerDef() throws Exception {
      // create the test instance
      Team team = new Team();
      team.setDescription("This is a D\u00E0scription");  // \u00E0 == � - ISOLatin1AccentFilterFactory should strip of diacritic
      team.setLocation("Atlanta");
      team.setName("ATL team");

      // persist and index the test object
      cache.put("id", team);
      SearchManager searchManager = Search.getSearchManager(cache);

      // execute several search to show that the right tokenizers were applies
      TermQuery query = new TermQuery(new Term("description", "D\u00E0scription"));
      assertEquals(
            "iso latin filter should work.  � should be a now", 0, searchManager.getQuery(query).list().size()
      );

      query = new TermQuery(new Term("description", "is"));
      assertEquals(
            "stop word filter should work. is should be removed", 0, searchManager.getQuery(query).list().size()
      );

      query = new TermQuery(new Term("description", "dascript"));
      assertEquals(
            "snowball stemmer should work. 'dascription' should be stemmed to 'dascript'",
            1,
            searchManager.getQuery(query).list().size()
      );
   }

   /**
    * Tests the analyzers defined on {@link Team}.
    *
    * @throws Exception in case the test fails.
    */
   public void testAnalyzers() throws Exception {
      SearchManager search = Search.getSearchManager(cache);

      Analyzer analyzer = search.getAnalyzer("standard_analyzer");
      String text = "This is just FOOBAR's";
      assertEquals(asList("This", "is", "just", "FOOBAR's"), terms(analyzer, "name", text));

      analyzer = search.getAnalyzer("html_standard_analyzer");
      text = "This is <b>foo</b><i>bar's</i>";
      assertEquals(asList("This", "is", "foobar's"), terms(analyzer, "name", text));

      analyzer = search.getAnalyzer("html_whitespace_analyzer");
      text = "This is <b>foo</b><i>bar's</i>";
      assertEquals(asList("This", "is", "foobar's"), terms(analyzer, "name", text));

      analyzer = search.getAnalyzer("length_analyzer");
      text = "ab abc abcd abcde abcdef";
      assertEquals(asList("abc", "abcd", "abcde"), terms(analyzer, "name", text));

      analyzer = search.getAnalyzer("length_analyzer");
      text = "ab abc abcd abcde abcdef";
      assertEquals(asList("abc", "abcd", "abcde"), terms(analyzer, "name", text));

      analyzer = search.getAnalyzer("porter_analyzer");
      text = "bikes bikes biking";
      assertEquals(asList("bike", "bike", "bike"), terms(analyzer, "name", text));

      analyzer = search.getAnalyzer("word_analyzer");
      text = "CamelCase";
      assertEquals(asList("Camel", "Case"), terms(analyzer, "name", text));

      analyzer = search.getAnalyzer("synonym_analyzer");
      text = "ipod cosmos";
      assertEquals(asList("ipod", "i-pod", "cosmos", "universe"), terms(analyzer, "name", text));

      analyzer = search.getAnalyzer("shingle_analyzer");
      text = "please divide this sentence into shingles";
      assertEquals(asList(
            "please",
            "please divide",
            "divide",
            "divide this",
            "this",
            "this sentence",
            "sentence",
            "sentence into",
            "into",
            "into shingles",
            "shingles"), terms(analyzer, "name", text));

      analyzer = search.getAnalyzer("pattern_analyzer");
      text = "foo,bar";
      assertEquals(asList("foo", "bar"), terms(analyzer, "name", text));

      // CharStreamFactories test
      analyzer = search.getAnalyzer("mapping_char_analyzer");
      text = "CORA\u00C7\u00C3O DE MEL\u00C3O";
      assertEquals(asList("CORACAO", "DE", "MELAO"), terms(analyzer, "name", text));
   }

   private List<String> terms(Analyzer analyzer, String fieldName, String text) throws IOException {
      List<String> terms = new ArrayList<>();
      TokenStream tokenStream = analyzer.tokenStream(fieldName, text);
      tokenStream.addAttribute(CharTermAttribute.class);
      CharTermAttribute attribute = tokenStream.getAttribute(CharTermAttribute.class);
      tokenStream.reset();
      while (tokenStream.incrementToken()) {
         terms.add(attribute.toString());
      }
      tokenStream.close();
      return terms;
   }

   protected Class<?>[] getAnnotatedClasses() {
      return new Class[]{
            Team.class
      };
   }

}
