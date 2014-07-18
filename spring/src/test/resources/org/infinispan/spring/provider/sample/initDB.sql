CREATE TABLE books (
  id     INTEGER     NOT NULL IDENTITY PRIMARY KEY,
  isbn   VARCHAR(30) NOT NULL,
  author VARCHAR(30) NOT NULL,
  title  VARCHAR(50) NOT NULL
);
CREATE INDEX books_isbn ON books (isbn);
