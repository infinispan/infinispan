import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.DefaultCacheManager;

// Create caches that will store the index. Here the programmatic configuration is used
DefaultCacheManager defaultCacheManager = new DefaultCacheManager();
Cache metadataCache = defaultCacheManager.getCache("metadataCache");
Cache dataCache = defaultCacheManager.getCache("dataCache");
Cache lockCache = defaultCacheManager.getCache("lockCache");

// Create the directory
Directory directory = DirectoryBuilder.newDirectoryInstance(metadataCache, dataCache, lockCache, indexName).create();

// Use the directory in Lucene
IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);

// Index a single document
Document doc = new Document();
doc.add(new StringField("field", "value", Field.Store.NO));
indexWriter.addDocument(doc);
indexWriter.close();

// Querying the inserted document
DirectoryReader directoryReader = DirectoryReader.open(directory);
IndexSearcher searcher = new IndexSearcher(directoryReader);
TermQuery query = new TermQuery(new Term("field", "value"));
TopDocs topDocs = searcher.search(query, 10);
System.out.println(topDocs.totalHits);
