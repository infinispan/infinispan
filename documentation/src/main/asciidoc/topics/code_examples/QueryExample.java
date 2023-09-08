// Create an Ickle query that performs a full-text search using the ':' operator on the 'title' and 'authors.name' fields
// You can perform full-text search only on indexed caches
Query<Book> fullTextQuery = cache.query("FROM org.infinispan.sample.Book b WHERE b.title:'infinispan' AND b.authors.name:'sanne'");

// Use the '=' operator to query fields in caches that are indexed or not
// Non full-text operators apply only to fields that are not analyzed
Query<Book> exactMatchQuery= cache.query("FROM org.infinispan.sample.Book b WHERE b.isbn = '12345678' AND b.authors.name : 'sanne'");

// You can use full-text and non-full text operators in the same query
Query<Book> query= cache.query("FROM org.infinispan.sample.Book b where b.authors.name : 'Stephen' and b.description : (+'dark' -'tower')");

// Get the results
List<Book> found=query.execute().list();
