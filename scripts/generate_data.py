import random
import pandas as pd
import faker
import string
from datetime import datetime, timedelta

fake = faker.Faker()

# Function to generate users
def generate_users(num_users):
    users = []
    for _ in range(num_users):
        user_id = random.randint(10000,99999)
        user_name = fake.name()
        user_age = random.randint(18, 70)
        user_country = fake.country()
        created_at = fake.date_this_decade()
        users.append({
            'user_id': user_id,
            'user_name': user_name,
            'user_age': user_age,
            'user_country': user_country,
            'created_at': created_at
        })
    return pd.DataFrame(users)

# Function to generate tracks
def generate_tracks(num_tracks):
    tracks = []
    genres = ['Pop', 'Rock', 'Hip Hop', 'Jazz', 'Electronic', 'Classical']
    for i in range(num_tracks):
        track_id = ''.join(random.choices(string.ascii_letters + string.digits, k=22))
        artists = fake.name()
        album_name = fake.word() + " " + fake.word()
        track_name = fake.sentence(nb_words=3).strip('.')
        popularity = random.randint(0, 100)
        duration_ms = random.randint(120000, 300000)
        explicit = random.choice([True, False])
        danceability = random.uniform(0, 1)
        energy = random.uniform(0, 1)
        key = random.randint(0, 11)
        loudness = random.uniform(-60, 0)
        mode = random.choice([0, 1])
        speechiness = random.uniform(0, 1)
        acousticness = random.uniform(0, 1)
        instrumentalness = random.uniform(0, 1)
        liveness = random.uniform(0, 1)
        valence = random.uniform(0, 1)
        tempo = random.uniform(60, 180)
        time_signature = random.choice([3, 4])
        track_genre = random.choice(genres)
        tracks.append({
            'id': i+1,
            'track_id': track_id,
            'artists': artists,
            'album_name': album_name,
            'track_name': track_name,
            'popularity': popularity,
            'duration_ms': duration_ms,
            'explicit': explicit,
            'danceability': danceability,
            'energy': energy,
            'key': key,
            'loudness': loudness,
            'mode': mode,
            'speechiness': speechiness,
            'acousticness': acousticness,
            'instrumentalness': instrumentalness,
            'liveness': liveness,
            'valence': valence,
            'tempo': tempo,
            'time_signature': time_signature,
            'track_genre': track_genre
        })
    return pd.DataFrame(tracks)

# Function to generate streams
def generate_streams(users_df, tracks_df, num_streams):
    streams = []
    for _ in range(num_streams):
        user_id = random.choice(users_df['user_id'])
        track_id = random.choice(tracks_df['track_id'])
        listen_time = fake.date_time_between(datetime(datetime.now().year,1,1), datetime.now())
        streams.append({
            'user_id': user_id,
            'track_id': track_id,
            'listen_time': listen_time
        })
    return pd.DataFrame(streams)

# Main function to generate the dataset
def generate_data(num_users, num_tracks, num_streams):
    users_df = generate_users(num_users)
    tracks_df = generate_tracks(num_tracks)
    streams_df = generate_streams(users_df, tracks_df, num_streams)
    
    return users_df, tracks_df, streams_df

# Example usage to generate the data
if __name__ == "__main__":
    num_users = 114586
    num_tracks = 500
    num_streams = 326645

    users_df, tracks_df, streams_df = generate_data(num_users, num_tracks, num_streams)

    # Save to CSV (or any other format you prefer)
    users_df.to_csv('users.csv', index=False)
    tracks_df.to_csv('tracks.csv', index=False)
    streams_df.to_csv('streams.csv', index=False)

    print(f"Generated {num_users} users, {num_tracks} tracks, and {num_streams} streams.")
