import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#CONFIG VARIABLES
LOG_DATA = config.get('S3','LOG_DATA')
ARN = config.get('IAM_ROLE','ARN')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES
## DWH Staging
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                artist varchar,
                                auth varchar,
                                firstname varchar,
                                gender char(1),
                                iteminsession int,
                                lastname varchar,
                                length float,
                                level varchar,
                                location text,
                                method varchar,
                                page varchar,
                                registration varchar,
                                sessionid int,
                                song varchar,
                                status int,
                                ts varchar,
                                useragent text,
                                userid int

                            )"""
                            )

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs ( 
                                artist_id varchar,
                                artist_latitude float,
                                artist_location varchar(max),
                                artist_longitude float,
                                artist_name varchar(max),
                                duration float,
                                num_songs int,
                                song_id varchar,
                                title varchar(max),
                                year int
                            )"""
                            )

## DWH Tables for analysis in Star Schema 
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS fact_songplays (
                            songplay_id INT IDENTITY(1, 1),
                            start_time timestamp NOT NULL,
                            user_id int NOT NULL,
                            level varchar,
                            song_id varchar,
                            artist_id varchar,
                            session_id int,
                            location varchar,
                            user_agent varchar,
                            PRIMARY KEY (songplay_id))"""
                       )
user_table_create = ("""CREATE TABLE IF NOT EXISTS _dim_users (
                        user_id int,
                        first_name varchar,
                        last_name varchar,
                        gender varchar,
                        level varchar,
                        PRIMARY KEY (user_id))"""
                    )
song_table_create = ("""CREATE TABLE IF NOT EXISTS dim_songs (
                        song_id varchar,
                        title varchar,
                        artist_id varchar,
                        year int,
                        PRIMARY KEY (song_id))"""
                    )
artist_table_create = 	("""CREATE TABLE IF NOT EXISTS dim_artists (
                            artist_id varchar NOT NULL,
                            name varchar,
                            location varchar,
                            latitude float,
                            longitude float,
                            PRIMARY KEY (artist_id))"""
                        )
time_table_create = ("""CREATE TABLE IF NOT EXISTS dim_time (
                        start_time timestamp,
                        hour int,
                        day int,
                        week int,
                        month int,
                        year int,
                        weekday int, 
                        PRIMARY KEY (start_time))"""
                    )

# STAGING TABLES
## Load data from S3 bucket
staging_events_copy = ("""copy staging_events 
                        from {0}
                        iam_role {1}
                        compupdate off
                        region 'us-west-2'
                        json {2};"""
                    ).format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs 
                        from {0}
                        iam_role {1}                        
                        compupdate off
                        json as 'auto ignorecase'
                        region 'us-west-2';"""
                    ).format(SONG_DATA, ARN)

# FINAL TABLES
## inser data into DWH Tables for analysis
songplay_table_insert = ("""INSERT INTO fact_songplays (
                                start_time, 
                                user_id,
                                level,
                                song_id,
                                artist_id,
                                session_id,
                                location,
                                user_agent) 
                            SELECT DISTINCT
                                e.ts AS start_time, 
                                e.userId AS user_id, 
                                e.level, 
                                s.song_id, 
                                s.artist_id, 
                                e.sessionId AS session_id, 
                                e.location, 
                                e.userAgent AS user_agent
                            FROM staging_events e JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
                            WHERE e.page = 'NextSong'"""
                        )

user_table_insert = ("""INSERT INTO dim_users (
                            user_id, 
                            first_name,
                            last_name,
                            gender,
                            level)
                        SELECT DISTINCT e.userId, 
                            e.firstName, 
                            e.lastName, 
                            e.gender, 
                            e.level
                        FROM staging_events e
                        WHERE page = 'NextSong' 
                        AND e.userId IS NOT NULL"""
                    )

song_table_insert = ("""INSERT INTO dim_songs (
                            song_id, 
                            title,
                            artist_id,
                            year,
                            duration)
                        SELECT DISTINCT s.song_id, 
                            s.title, 
                            s.artist_id, 
                            s.year, 
                            s.duration
                        FROM staging_songs s
                        WHERE s.song_id IS NOT NULL"""
                    )

artist_table_insert =   ("""INSERT INTO artists (
                                artist_id, 
                                name,
                                location,
                                latitude,
                                longitude) 
                            SELECT DISTINCT s.artist_id, 
                                s.artist_name, 
                                s.artist_location, 
                                s.artist_latitude, 
                                s.artist_longitude
                            FROM staging_songs s
                            WHERE s.artist_id IS NOT NULL"""
                        )

time_table_insert = ("""INSERT INTO time (
                            start_time, 
                            hour,
                            day,
                            week,
                            month,
                            year,
                            weekday) 
                        SELECT s.start_time, 
                            extract(hour from s.start_time), 
                            extract(day from s.start_time), 
                            extract(week from s.start_time), 
                            extract(month from s.start_time), 
                            extract(year from s.start_time), 
                            extract(weekday from s.start_time)
                        FROM songplays s"""
                    )

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
