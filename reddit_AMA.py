import os
import praw
import pandas as pd
import datetime as dt
from tqdm import tqdm
import time


def get_date(created):
    return dt.datetime.fromtimestamp(created)


def reddit_connection():
    personal_use_script = os.environ["REDDIT_PERSONAL_USE_SCRIPT_14_CHARS"]
    client_secret = os.environ["REDDIT_SECRET_KEY_27_CHARS"]
    user_agent = os.environ["REDDIT_APP_NAME"]
    username = os.environ["REDDIT_USER_NAME"]
    password = os.environ["REDDIT_LOGIN_PASSWORD"]

    reddit = praw.Reddit(client_id=personal_use_script, \
                         client_secret=client_secret, \
                         user_agent=user_agent, \
                         username=username, \
                         password='')
    return reddit

def get_questions_answers(link):
    QA_list = []
    sub = reddit.submission(url = link)
    main_author = sub.author

    for top_level_comment in sub.comments:
        try:
            question = top_level_comment.body
            for reply in top_level_comment.replies:
                try:
                    if reply.author == main_author:
                        answer = reply.body
                        QA_list.append((question, answer))
                except:
                    continue
        except:
            continue

    return QA_list

def get_comments_answers(reddit, search_words, items_limit=5000):

    # Collect reddit question and answers
    subreddit = reddit.subreddit(search_words)
    new_subreddit = subreddit.new(limit=items_limit)
    QA_dict = { "title":[],
                "score":[],
                "id":[],
                "url":[],
                "comms_num": [],
                "created": [],
                "body":[],
                "question count": [],
                "question":[],
                "answer":[],
                "QA_ID":[],
                "subreddit":[]}

    print(f"retreive new reddit posts ...")
    for submission in tqdm(new_subreddit):
        try:
            QA_list = get_questions_answers(submission.url)
        except:
            print("Failed to do this one " + submission.url)
            continue
        if len(QA_list) == 0 or QA_list == None or submission.title == 'Comment':
            continue
        else:
            i = 1 #for question/answer ID
            for QA_tuple in QA_list:
                QA_dict["question"].append(QA_tuple[0])
                QA_dict['answer'].append(QA_tuple[1])
                QA_dict['QA_ID'].append(i)
                QA_dict["subreddit"].append(search_words)
                QA_dict["question count"].append(len(QA_list))
                QA_dict["title"].append(submission.title)
                QA_dict["score"].append(submission.score)
                QA_dict["id"].append(submission.id)
                QA_dict["url"].append(submission.url)
                QA_dict["comms_num"].append(submission.num_comments)
                QA_dict["created"].append(submission.created)
                QA_dict["body"].append(submission.selftext)
                i+=1

    QA_df = pd.DataFrame(QA_dict)
    print(f"new reddit posts retrieved: {len(QA_df)}")
    QA_df['timestamp'] = QA_df['created'].apply(lambda x: get_date(x))

    return QA_df


def update_and_save_dataset(topics_df):
    file_path = "reddit_AMA_full_export.csv"
    if os.path.exists(file_path):
        topics_old_df = pd.read_csv(file_path)
        print(f"past reddit posts: {topics_old_df.shape}")
        topics_all_df = pd.concat([topics_old_df, topics_df], axis=0)
        print(f"new reddit posts: {topics_df.shape[0]} past posts: {topics_old_df.shape[0]} all posts: {topics_all_df.shape[0]}")
        topics_new_df = topics_all_df.drop_duplicates(subset = ["id"], keep='last', inplace=False)
        print(f"all reddit posts: {topics_new_df.shape}")
        topics_new_df.to_csv(file_path, index=False)
    else:
        print(f"reddit posts: {topics_df.shape}")
        topics_df.to_csv(file_path, index=False)


if __name__ == "__main__":
    reddit = reddit_connection()
    topics_data_df = get_comments_answers(reddit, 'IAmA')
    topics_data_df = pd.concat([topics_data_df, get_comments_answers(reddit, 'AMA')])
    update_and_save_dataset(topics_data_df)
