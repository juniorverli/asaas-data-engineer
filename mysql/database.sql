use db

CREATE TABLE config(
    name varchar(50),
    offset int
);

INSERT INTO config (name, offset) 
VALUES ('characters', 0), ('comics', 0);

CREATE TABLE characters(
    id int,
    name varchar(100),
    description varchar(5000),
    modified datetime,
    resourceURI varchar(500),
    thumbnail varchar(500),
    comicId int
);


CREATE TABLE comics(
    id int,
    digitalid int,
    title varchar(500),
    issueNumber float,
    variantDescription varchar(500),
    description varchar(5000),
    modified datetime,
    isbn varchar(500),
    upc varchar(500),
    diamondCode varchar(500),
    ean varchar(500),
    issn varchar(500),
    format varchar(500),
    pageCount int,
    resourceURI varchar(500),
    thumbnail varchar(500)
);