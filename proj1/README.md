# Project 1: Data Modeling with Postgres

As elucidated after performing requirements gathering with the various stakeholders of the business, Sparkify, there exists a need for a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis.

## Purpose

This database design allows Sparkify to better understand their business in an efficient manner, as implemented through the "star schema" design.

This allows Sparkify to answer analytical questions including:

- Which songs are played the most?
- Which artists are most popular?
- When are songs typically played (e.g. night or day)?

## Design

The database design follows the "star schema" design, with two key components: a fact table and various dimension tables.

In this context:

1. Fact table
    - `songplays`: describing the event of a song being played
      - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

2. Dimension table
    - `users`: users in the application
        - *user_id, first_name, last_name, gender, level*
    - `songs`: describes songs
        - *song_id, title, artist_id, year, duration*
    - `artists`: describes artists
        - *artist_id, name, location, lattitude, longitude*
    - `time`: gives additional granularity for reporting on time
        - *start_time, hour, day, week, month, year, weekday*

The "star schema" allows for very efficient reporting as a denormalised structure. As compared to a normalised, third normal form (3NF) structure typically used for transaction workloads, including those powering the application and emitting the JSON logs, the "star schema" is much easier and more efficient for analysts to query.

## Instructions

Run `bash run_job.sh`.

This does the following in order:
- Drops tables if existing
- Creates tables with schemas
- Reads the log and song data JSON files
- Inserts them into the tables

## Files

1. `create_tables.py`: 

    Creates the tables using SQL queries in `sql_queries.py`.
    
2. `etl.py`: 
    
    Contains Python code to:
    - Read the JSON files
    - Process the data
    - Run the `INSERT` statements

3. `sql_queries.py`:

    Contains Python code detailing SQL queries to:
    - `INSERT` data into tables
    - `CREATE` tables
    - `DROP` tables if existing
    - `SELECT` songId and artistId for a lookup to insert into the `songplays` fact table.

4. `etl.ipynb`:

    Jupyter Notebook to test ETL code.

5. `test.ipynb`:

    Tests if the `INSERT` statements ran correctly by performing a `SELECT ... FROM <table> LIMIT 5` statement.


