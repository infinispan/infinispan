CREATE TABLE books (
    isbn NUMBER(13),
    reprint INT,
    title varchar(120),
    author varchar(80)
    PRIMARY KEY(isbn, reprint)
);
