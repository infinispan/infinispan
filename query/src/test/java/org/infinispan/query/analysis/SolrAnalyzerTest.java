package org.infinispan.query.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.hibernate.search.util.AnalyzerUtils;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;

/**
 * Copied and adapted from Hibernate Search
 * org.hibernate.search.test.analyzer.solr.SolrAnalyzerTest
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 * @author Emmanuel Bernard
 * @author Hardy Ferentschik
 */
@Test(groups = "functional", testName = "query.analysis.SolrAnalyzerTest")
public class SolrAnalyzerTest extends SingleCacheManagerTest {

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
            .index(Index.ALL)
            .addProperty("hibernate.search.default.directory_provider", "ram")
            .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT");
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
      team.setDescription( "This is a D\u00E0scription" );  // \u00E0 == � - ISOLatin1AccentFilterFactory should strip of diacritic 
      team.setLocation( "Atlanta" );
      team.setName( "ATL team" );

      // persist and index the test object
      cache.put("id", team);
      SearchManager searchManager = Search.getSearchManager(cache);

      // execute several search to show that the right tokenizers were applies
      TermQuery query = new TermQuery( new Term( "description", "D\u00E0scription" ) );
      assertEquals(
            "iso latin filter should work.  � should be a now", 0, searchManager.getQuery( query ).list().size()
      );

      query = new TermQuery( new Term( "description", "is" ) );
      assertEquals(
            "stop word filter should work. is should be removed", 0, searchManager.getQuery( query ).list().size()
      );

      query = new TermQuery( new Term( "description", "dascript" ) );
      assertEquals(
            "snowball stemmer should work. 'dascription' should be stemmed to 'dascript'",
            1,
            searchManager.getQuery( query ).list().size()
      );
   }

   /**
    * Tests the analyzers defined on {@link Team}.
    *
    * @throws Exception in case the test fails.
    */
   public void testAnalyzers() throws Exception {
      SearchManager search = Search.getSearchManager(cache);

      Analyzer analyzer = search.getAnalyzer( "standard_analyzer" );
      String text = "This is just FOOBAR's";
      Token[] tokens = AnalyzerUtils.tokensFromAnalysis( analyzer, "name", text );
      assertTokensEqual( tokens, new String[] { "This", "is", "just", "FOOBAR's" } );

      analyzer = search.getAnalyzer( "html_standard_analyzer" );
      text = "This is <b>foo</b><i>bar's</i>";
      tokens = AnalyzerUtils.tokensFromAnalysis( analyzer, "name", text );
      assertTokensEqual( tokens, new String[] { "This", "is", "foobar's" } );

      analyzer = search.getAnalyzer( "html_whitespace_analyzer" );
      text = "This is <b>foo</b><i>bar's</i>";
      tokens = AnalyzerUtils.tokensFromAnalysis( analyzer, "name", text );
      assertTokensEqual( tokens, new String[] { "This", "is", "foobar's" } );

      analyzer = search.getAnalyzer( "length_analyzer" );
      text = "ab abc abcd abcde abcdef";
      tokens = AnalyzerUtils.tokensFromAnalysis( analyzer, "name", text );
      assertTokensEqual( tokens, new String[] { "abc", "abcd", "abcde" } );

      analyzer = search.getAnalyzer( "length_analyzer" );
      text = "ab abc abcd abcde abcdef";
      tokens = AnalyzerUtils.tokensFromAnalysis( analyzer, "name", text );
      assertTokensEqual( tokens, new String[] { "abc", "abcd", "abcde" } );

      analyzer = search.getAnalyzer( "porter_analyzer" );
      text = "bikes bikes biking";
      tokens = AnalyzerUtils.tokensFromAnalysis( analyzer, "name", text );
      assertTokensEqual( tokens, new String[] { "bike", "bike", "bike" } );

      analyzer = search.getAnalyzer( "word_analyzer" );
      text = "CamelCase";
      tokens = AnalyzerUtils.tokensFromAnalysis( analyzer, "name", text );
      assertTokensEqual( tokens, new String[] { "Camel", "Case" } );

      analyzer = search.getAnalyzer( "synonym_analyzer" );
      text = "ipod cosmos";
      tokens = AnalyzerUtils.tokensFromAnalysis( analyzer, "name", text );
      assertTokensEqual( tokens, new String[] { "ipod", "i-pod", "universe", "cosmos" } );

      analyzer = search.getAnalyzer( "shingle_analyzer" );
      text = "please divide this sentence into shingles";
      tokens = AnalyzerUtils.tokensFromAnalysis( analyzer, "name", text );
      assertTokensEqual(
            tokens,
            new String[] {
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
                  "shingles"
            }
      );

      analyzer = search.getAnalyzer( "pattern_analyzer" );
      text = "foo,bar";
      tokens = AnalyzerUtils.tokensFromAnalysis( analyzer, "name", text );
      assertTokensEqual( tokens, new String[] { "foo", "bar" } );

      // CharStreamFactories test
      analyzer = search.getAnalyzer( "mapping_char_analyzer" );
      text = "CORA\u00C7\u00C3O DE MEL\u00C3O";
      tokens = AnalyzerUtils.tokensFromAnalysis( analyzer, "name", text );
      assertTokensEqual( tokens, new String[] { "CORACAO", "DE", "MELAO" } );
   }

   protected Class<?>[] getAnnotatedClasses() {
      return new Class[] {
            Team.class
      };
   }

   private static void assertTokensEqual(Token[] tokens, String[] strings) {
      assertEquals( strings.length, tokens.length );

      for ( int i = 0; i < tokens.length; i++ ) {
         assertEquals( "index " + i, strings[i], AnalyzerUtils.getTermText( tokens[i] ) );
      }
   }

}
