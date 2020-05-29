// get the query factory from the cache:
QueryFactory queryFactory = org.infinispan.query.Search.getQueryFactory(cache);

// create an Ickle query that will do a full-text search (operator ':') on fields 'title' and 'authors.name'
Query<Book> fullTextQuery = queryFactory.create("FROM com.acme.Book WHERE title:'infinispan' AND authors.name:'sanne'")

// The ('=') operator is not a full-text operator, thus can be used in both indexed and non-indexed caches
Query<Book> exactMatchQuery = queryFactory.create("FROM com.acme.Book WHERE title = 'Programming Infinispan' AND authors.name = 'Sanne Grinnovero'")

// Full-text and non-full text operators can be part of the same query
Query q = queryFactory.create("FROM com.query.Book b where b.author.name = 'Stephen' and b.description : (+'dark' -'tower')");

// get the results
List<Book> found=query.execute().list();