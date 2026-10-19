-- Databricks notebook source
--DDL
--data type changes as Oracle and Databricks are different
--varcher to string
--number(n) to int (when n <= 10) / boolean( n=1 specific when n only have two values 0 or 1) / bigint ( when n > 10 or no constraint for number)
--DOUBLE to double

DROP TABLE IF EXISTS ARTIST_GENRE;
DROP TABLE IF EXISTS TRACK_ARTIST;
DROP TABLE IF EXISTS TRACK_FEATURE;
DROP TABLE IF EXISTS ARTIST;
DROP TABLE IF EXISTS GENRE;
DROP TABLE IF EXISTS TRACK;
DROP TABLE IF EXISTS TRACK_KEY;

CREATE TABLE ARTIST 
    ( 
     artist_id         STRING  NOT NULL , 
     artist_name       STRING , 
     followers         BIGINT , 
     artist_popularity INT 
    ) 
;

ALTER TABLE ARTIST 
    ADD CONSTRAINT ARTIST_PK PRIMARY KEY ( artist_id ) ;

CREATE TABLE IF NOT EXISTS ARTIST_GENRE 
    ( 
     artist_id STRING NOT NULL , 
     genre_id  BIGINT  NOT NULL 
    ) 
;

ALTER TABLE ARTIST_GENRE 
    ADD CONSTRAINT ARTIST_GENRE_PK PRIMARY KEY ( artist_id, genre_id ) ;

CREATE TABLE GENRE 
    ( 
     genre_id   BIGINT  NOT NULL , 
     genre_name STRING 
    ) 
;

ALTER TABLE GENRE 
    ADD CONSTRAINT GENRE_PK PRIMARY KEY ( genre_id ) ;

CREATE TABLE TRACK 
    ( 
     track_id         STRING  NOT NULL , 
     track_name       STRING  , 
     track_popularity INT , 
     duration_ms      INT , 
     explicit         BOOLEAN , 
     release_date     DATE 
    ) 
;

ALTER TABLE TRACK 
    ADD CONSTRAINT TRACKS_PK PRIMARY KEY ( track_id ) ;

CREATE TABLE TRACK_ARTIST 
    ( 
     track_id  STRING NOT NULL , 
     artist_id STRING NOT NULL 
    ) 
;

ALTER TABLE TRACK_ARTIST 
    ADD CONSTRAINT TRACK_ARTIST_PK PRIMARY KEY ( track_id, artist_id ) ;

CREATE TABLE TRACK_FEATURE 
    ( 
     track_id         STRING NOT NULL , 
     danceability     DOUBLE , 
     energy           DOUBLE , 
     key_id           BIGINT NOT NULL , 
     loudness         DOUBLE , 
     modality         BOOLEAN , 
     speechiness      DOUBLE , 
     acousticness     DOUBLE , 
     instrumentalness DOUBLE , 
     liveness         DOUBLE , 
     valence          DOUBLE , 
     tempo            DOUBLE , 
     time_signature   INT
    ) 
;

ALTER TABLE TRACK_FEATURE 
    ADD CONSTRAINT TRACK_FEATURES_PK PRIMARY KEY ( track_id ) ;

CREATE TABLE TRACK_KEY 
    ( 
     key_id   BIGINT NOT NULL , 
     key_name STRING
    ) 
;

ALTER TABLE TRACK_KEY 
    ADD CONSTRAINT TRACK_KEY_PK PRIMARY KEY ( key_id ) ;

ALTER TABLE ARTIST_GENRE 
    ADD CONSTRAINT ARTIST_GENRE_FK_ARTIST FOREIGN KEY 
    ( 
     artist_id
    ) 
    REFERENCES ARTIST 
    ( 
     artist_id
    ) 
;

ALTER TABLE ARTIST_GENRE 
    ADD CONSTRAINT ARTIST_GENRE_FK_GENRE FOREIGN KEY 
    ( 
     genre_id
    ) 
    REFERENCES GENRE 
    ( 
     genre_id
    ) 
;

ALTER TABLE TRACK_ARTIST 
    ADD CONSTRAINT TRACK_ARTIST_FK_ARTIST FOREIGN KEY 
    ( 
     artist_id
    ) 
    REFERENCES ARTIST 
    ( 
     artist_id
    ) 
;

ALTER TABLE TRACK_ARTIST 
    ADD CONSTRAINT TRACK_ARTIST_FK_TRACK FOREIGN KEY 
    ( 
     track_id
    ) 
    REFERENCES TRACK 
    ( 
     track_id
    ) 
;

ALTER TABLE TRACK_FEATURE 
    ADD CONSTRAINT TRACK_FEATURE_FK_TRACK FOREIGN KEY 
    ( 
     track_id
    ) 
    REFERENCES TRACK 
    ( 
     track_id
    ) 
;

ALTER TABLE TRACK_FEATURE 
    ADD CONSTRAINT TRACK_FEATURE_FK_TRACK_KEY FOREIGN KEY 
    ( 
     key_id
    ) 
    REFERENCES TRACK_KEY 
    ( 
     key_id
    ) 
;


