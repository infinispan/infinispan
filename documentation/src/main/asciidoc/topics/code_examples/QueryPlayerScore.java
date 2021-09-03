QueryFactory queryFactory = Search.getQueryFactory(playersScores);
Query topTenQuery = queryFactory
  .create("from com.redhat.PlayerScore ORDER BY p.score DESC, p.timestamp ASC")
  .maxResults(10);
List<PlayerScore> topTen = topTenQuery.execute().list();
