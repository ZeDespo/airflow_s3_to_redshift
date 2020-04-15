class SqlQueries:
    
    songplays_table_create = ("""
        CREATE TABLE IF NOT EXISTS public.songplays (
            songplay_id varchar(32) NOT NULL,
            start_time timestamp NOT NULL,
            user_id varchar NOT NULL,
            "level" varchar(256),
            song_id varchar(256),
            artist_id varchar(256),
            session_id int4,
            location varchar(256),
            user_agent varchar(256),
            CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
        )
    """)
    
    songplays_table_insert = ("""
        INSERT INTO songplays (
            songplay_id,
            start_time, 
            user_id,
            level, 
            song_id, 
            artist_id,
            session_id,
            location,
            user_agent
        )
        (
            SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userId, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionId, 
                events.location, 
                events.userAgent
                FROM (
                SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                    FROM staging_logs
                    WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
        )
    """)
    
    user_table_create = """
        CREATE TABLE IF NOT EXISTS public.users (
            user_id varchar NOT NULL,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            "level" varchar(4),
            CONSTRAINT users_pkey PRIMARY KEY (user_id)
        );
    """

    user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT userId, firstName, lastName, gender, level
        FROM staging_logs
        GROUP BY userId, firstName, lastName, gender, level
        ORDER BY MAX(id) DESC
    """)

    song_table_create = """
        CREATE TABLE IF NOT EXISTS public.songs (
            song_id varchar(256) NOT NULL,
            title varchar(256),
            artist_id varchar(256),
            "year" int4,
            duration numeric(18,0),
            CONSTRAINT songs_pkey PRIMARY KEY (song_id)
        );
    """
    song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs
    """)
    
    artist_table_create = """
    CREATE TABLE IF NOT EXISTS public.artists (
        artist_id varchar(256) NOT NULL,
        name varchar(256),
        location varchar(256),
        latitude numeric(18,0),
        longitude numeric(18,0),
        CONSTRAINT artists_pkey PRIMARY KEY (artist_id)
    )"""

    artist_table_insert = ("""
        INSERT INTO artists (
            artist_id, 
            name, 
            location, 
            latitude, 
            longitude
        )
        SELECT 
            artist_id, 
            artist_name, 
            artist_location, 
            artist_latitude, 
            artist_longitude 
        FROM staging_songs
        GROUP BY artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        ORDER BY MAX(id) DESC;
    """)
    
    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time (
            start_time timestamp NOT NULL,
            hour int,
            day int, 
            week int, 
            month int, 
            year int, 
            weekday varchar,
            CONSTRAINT time_pkey PRIMARY KEY (start_time)  
        );
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT 
            start_time, 
            extract(hour from start_time), 
            extract(day from start_time), 
            extract(week from start_time), 
            extract(month from start_time), 
            extract(year from start_time), 
            extract(dayofweek from start_time)
        FROM songplays
    """)
    
    create_staging_log_table = """
        CREATE TABLE IF NOT EXISTS staging_logs (
            id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
            artist varchar,
            auth varchar,
            firstName varchar,
            gender varchar, 
            itemInSession int, 
            lastName varchar, 
            length numeric, 
            level varchar, 
            location varchar,
            method varchar,
            page varchar,
            registration numeric, 
            sessionId int,
            song varchar, 
            status int, 
            ts bigint,
            userAgent varchar,
            userId varchar
        )
    """

    create_staging_song_table = """
        CREATE TABLE IF NOT EXISTS staging_songs (
            id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
            num_songs int,
            artist_id varchar,
            artist_latitude real,
            artist_longitude real, 
            artist_location varchar,
            artist_name varchar,
            song_id varchar,
            title varchar,
            duration numeric,
            year int
        )
    """

    copy_from_s3_to_staging = ("""
        COPY {}
        FROM '{}'
        IAM_ROLE '{}'
        REGION 'us-west-2'
        JSON '{}'
    """)
    
    delete_conflicting_rows = """
        DELETE FROM {}
        USING {}
        WHERE {}.{} = {}.{}
    """