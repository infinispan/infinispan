package org.infinispan.test.integration.as;

import com.google.common.base.Joiner;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Terms;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.Version;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;


/**
 * Test infinispan-lucene-directory server module
 *
 * @author gustavonalle
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class InfinispanLuceneDirectoryIT {

   private Directory directory;
   private EmbeddedCacheManager cacheManager;
   private Cache<?, ?> cache;
   private static final org.apache.lucene.util.Version LUCENE_VERSION = luceneVersion();

   @Before
   public void setup() {
      cacheManager = new DefaultCacheManager();
      cache = cacheManager.getCache();
      directory = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "index").create();
   }

   @After
   public void tearDown() throws IOException {
      directory.close();
      cache.stop();
      cacheManager.stop();
   }

   @Deployment
   public static Archive<?> deployment() {
      String dependencies = deps(
            dep("org.infinispan", Version.MODULE_SLOT),
            dep("org.infinispan.lucene-directory", Version.MODULE_SLOT)
      );
      StringAsset manifest = new StringAsset(
            Descriptors.create(ManifestDescriptor.class).attribute("Dependencies", dependencies).exportAsString());
      return ShrinkWrap.create(WebArchive.class, "lucene.war")
            .addClass(InfinispanLuceneDirectoryIT.class)
            .add(manifest, "META-INF/MANIFEST.MF");
   }

   @Test
   public void testCoreLucene4() throws IOException {
      Document document = buildLuceneDoc("field1", "The quick brown fox jumped over the lazy dog");
      index(document);

      assertEquals(9, terms("field1").size());
   }

   @Test
   public void testQParserLucene4() throws IOException, ParseException {
      Document document = buildLuceneDoc("field1", "The quick brown fox jumped over the lazy dog");
      index(document);
      Query matchingQuery = buildQuery("field1:box~");
      Query nonMatchingQuery = buildQuery("-field1:over");
      IndexSearcher indexSearcher = openSearcher();

      assertEquals(1, indexSearcher.search(matchingQuery, 1).totalHits);
      assertEquals(0, indexSearcher.search(nonMatchingQuery, 1).totalHits);
   }

   @Test
   public void testGrouping() throws IOException {
      Document doc1 = buildLuceneDoc("field1", "value1");
      Document doc2 = buildLuceneDoc("field1", "value1");
      Document doc3 = buildLuceneDoc("field1", "value2");
      index(doc1, doc2, doc3);
      GroupingSearch groupingSearch = new GroupingSearch("field1");
      TopGroups<Object> topGroups = groupingSearch.search(openSearcher(), new MatchAllDocsQuery(), 0, 10);

      assertEquals(3, topGroups.totalHitCount);
      assertEquals(2, topGroups.groups.length);
   }

   @Test
   public void testFaceting() throws Exception {
      Directory taxonomyDirectory =
            DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "taxonomy").create();
      DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(taxonomyDirectory);
      FacetsConfig cfg = new FacetsConfig();
      Document doc1 = new Document();
      Document doc2 = new Document();
      Document doc3 = new Document();
      doc1.add(new FacetField("category", "c2"));
      doc2.add(new FacetField("category", "c2"));
      doc3.add(new FacetField("category", "c1"));
      index(cfg.build(tw, doc1), cfg.build(tw, doc2), cfg.build(tw, doc3));
      tw.close();
      DirectoryTaxonomyReader tr = new DirectoryTaxonomyReader(taxonomyDirectory);
      FacetsCollector fc = new FacetsCollector();
      FacetsCollector.search(openSearcher(), new MatchAllDocsQuery(), 10, fc);
      Facets facets = new FastTaxonomyFacetCounts(tr, cfg, fc);
      FacetResult category = facets.getTopChildren(10, "category");

      assertEquals(2, category.childCount);

   }

   private Query buildQuery(String q) throws ParseException {
      QueryParser queryParser = new QueryParser(LUCENE_VERSION, "field1", new WhitespaceAnalyzer(LUCENE_VERSION));
      return queryParser.parse(q);
   }

   private IndexSearcher openSearcher() throws IOException {
      return new IndexSearcher(DirectoryReader.open(directory));
   }

   private Terms terms(String field) throws IOException {
      DirectoryReader reader = DirectoryReader.open(directory);
      AtomicReaderContext readerContext = reader.getContext().leaves().iterator().next();
      return readerContext.reader().terms(field);
   }

   private void index(Document... documents) throws IOException {
      IndexWriterConfig iwc = new IndexWriterConfig(LUCENE_VERSION, new WhitespaceAnalyzer(LUCENE_VERSION));
      IndexWriter indexWriter = new IndexWriter(directory, iwc);
      for (Document doc : documents) {
         indexWriter.addDocument(doc);
      }
      indexWriter.close();
   }

   private Document buildLuceneDoc(String fieldName, String contents) {
      Document document = new Document();
      FieldType fieldType = new FieldType();
      fieldType.setIndexed(true);
      document.add(new Field(fieldName, contents, fieldType));
      return document;
   }

   private static org.apache.lucene.util.Version luceneVersion() {
      return nonDeprecatedConstants(org.apache.lucene.util.Version.class).iterator().next();
   }

   private static <E extends Enum<E>> List<E> nonDeprecatedConstants(Class<E> enumType) {
      List<E> results = new ArrayList<>();
      for (java.lang.reflect.Field f : enumType.getDeclaredFields()) {
         if (f.isEnumConstant() && !f.isAnnotationPresent(Deprecated.class)) {
            results.add(Enum.valueOf(enumType, f.getName()));
         }
      }
      return results;
   }

   private static String dep(String name, String version) {
      return name + ":" + version + " services";
   }

   private static String deps(String... dep) {
      return Joiner.on(", ").join(dep);
   }

}
