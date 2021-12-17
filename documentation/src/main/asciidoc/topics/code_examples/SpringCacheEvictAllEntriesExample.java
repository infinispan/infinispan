@Transactional
@CacheEvict (value="books", key = "#bookId", allEntries = true)
public void deleteAllBookEntries() {...}
