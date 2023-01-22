import pandas as pd
import tweepy
import boto3
import configparser
import psycopg2

config = configparser.ConfigParser()
config.read_file(open('cluster.config'))

# [AWS]
KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")

# [DWH]
DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_HOST = config.get("DWH", "DWH_HOST")

# [S3]
BUCKET = config.get("S3", "BUCKET")

redshift = boto3.client('redshift',
                        region_name="ca-central-1",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

s3 = boto3.resource('s3',
                    region_name="ca-central-1",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET)

iam = boto3.client('iam',
                   region_name="ca-central-1",
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)

bucket = s3.Bucket("dhruv-twitter-data-project")
log_data_files = [
    filename.key for filename in bucket.objects.filter(Prefix='')]


roelArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']


access_key = "ACCESS KEY"
access_secret = "SECRET KEY"
consumer_key = "CONSUMER KEY"
consumer_secret = "CONSUMER SECRET KEY"

# Twitter Auth
auth = tweepy.OAuthHandler(access_key, access_secret)
auth.set_access_token(consumer_key, consumer_secret)

api = tweepy.API(auth)

tweets = api.user_timeline(screen_name='@FullStack_Cafe',
                           count=200, include_rts=False, tweet_mode='extended')

text_col = ""


def run_twitter_etl():
    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        redefined_tweet = {"user": tweet.user.screen_name,
                           'text': text,
                           'created_at': tweet.created_at}

        tweet_list.append(redefined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv("interview_questions.csv")

    text_list = []
    for text in tweets:
        text = tweet._json["full_text"]

        text_list.append(text)

    text_df = pd.read_csv("./interview_questions.csv")
    text_col = text_df["text"]
    print(text_col)


def to_s3_bucket():
    for i in range(0, len(text_col), 10):
        set = text_col[i:i+10]

        with open(f"text{i}.txt", "w") as f:
            f.write("|".join(set))

    s3.meta.client.upload_file(
        f"./text{i}.txt", BUCKET, f"text{i}.txt")


def copy_to_redshift():

    conn = psycopg2.connect(
        host=DWH_HOST,
        port=DWH_PORT,
        dbname=DWH_DB,
        user=DWH_DB_USER,
        password=DWH_DB_PASSWORD
    )

    cur = conn.cursor()

    cur.execute("CREATE TABLE text_data (text VARCHAR(MAX))")

    for i in range(0, len(text_col), 10):
        cur.execute(
            f"COPY text_data FROM 's3://dhruv-twitter-data-project/text{i}.txt' CREDENTIALS 'aws_access_key_id={KEY};aws_secret_access_key={SECRET}' DELIMITER '|'")
