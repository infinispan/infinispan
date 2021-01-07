// get the query factory from the cache:
QueryFactory queryFactory = org.infinispan.query.Search.getQueryFactory(cache);

// create an Ickle query that will do a full-text search (operator ':') on fields 'title' and 'authors.name'
Query<Book> fullTextQuery = queryFactory.create("FROM org.infinispan.sample.Book b WHERE b.title:'infinispan' AND b.authors.name:'sanne'");

// The ('=') operator is not a full-text operator, thus can be used in both indexed and non-indexed caches
// Non full-text operators can only be applied in fields that are not analyzed
Query<Book> exactMatchQuery=queryFactory.create("FROM org.infinispan.sample.Book b WHERE b.isbn = '12345678' AND b.authors.name : 'sanne'");

// Full-text and non-full text operators can be part of the same query
Query<Book> query=queryFactory.create("FROM org.infinispan.sample.Book b where b.authors.name : 'Stephen' and b.description : (+'dark' -'tower')");

// get the results
List<Book> found=query.execute().list();