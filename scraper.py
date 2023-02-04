import time
import json
import datetime
import requests
import os
import yaml
import pandas as pd


class TweetSearch:

    def __init__(self, access_key_path: str, config_path: str) -> None:

        with open(access_key_path) as file:
            self.__key = yaml.safe_load(file)

        with open(config_path) as file:
            self.__config = yaml.safe_load(file)

        bearer_token = self.auth()

        if not os.path.exists("Data"):
            os.mkdir("Data")

        searched_df = TweetSearch.search(
            self.__config, TweetSearch.create_headers(bearer_token))
        
        output_file_name  = self.__config["start_time"] + "-" + self.__config["end_time"]
        searched_df.to_csv("./Data/" + output_file_name + ".csv")
        searched_df.to_pickle("./Data/" + output_file_name + ".pkl")

    def auth(self):
        return self.__key["Bearer Token"]

    @staticmethod
    def create_headers(bearer_token):
        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        return headers

    @staticmethod
    def connect_to_endpoint(url, headers, params, next_token=None):
        # params object received from create_url function
        params['next_token'] = next_token
        response = requests.request("GET", url, headers=headers, params=params)
        print("Endpoint Response Code: " + str(response.status_code))
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()

    @staticmethod
    def create_url(keyword, start_date, end_date, max_results=10):

        search_url = "https://api.twitter.com/2/tweets/search/all"

        print(keyword)
        query_params = {'query': keyword,
                        'start_time': start_date,
                        'end_time': end_date,
                        'max_results': max_results,
                        'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                        'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                        'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                        'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                        'next_token': {}}
        return (search_url, query_params)

    @staticmethod
    def search(config: dict, headers):

        max_results = config["max_results"]
        start = config['start_time']
        end = config['end_time']

        start_date = datetime.datetime(int(start[0:4]), int(
            start[5:7]), int(start[8:10]), int(start[11:13]), 00, 00)
        end_date = datetime.datetime(int(end[0:4]), int(
            end[5:7]), int(end[8:10]), int(end[11:13]), 00, 00)
        delta = datetime.timedelta(seconds=config["time_delta"]["seconds"],
                                   minutes=config["time_delta"]["minutes"],
                                   hours=config["time_delta"]["hours"],
                                   days=config["time_delta"]["days"])

        in_reply_to_user = []
        text = []
        lang = []
        tweet_id = []
        source = []
        conversation_id = []
        reply_settings = []
        retweet_count = []
        reply_count = []
        like_count = []
        quote_count = []
        author_id = []
        created_at = []

        loc = {}
        username = {}
        verified = {}

        location = []
        ver = []
        usern = []

        output_file = {"author_id": author_id, "username": usern, "verified": ver, "created_at": created_at, "tweet": text, "tweet_id": tweet_id,
                       "lang": lang, "like_count": like_count, "reply_count": reply_count, "retweet_count": retweet_count, "quote_count": quote_count, "Location": location}
        output_file = pd.DataFrame(output_file)

        while start_date < end_date:
            current = str(start_date)[:10] + "T" + \
                str(start_date)[11:] + ".000Z"
            start_date += delta
            next = str(start_date)[:10] + "T" + str(start_date)[11:] + ".000Z"

            url = TweetSearch.create_url(
                config["query"], current, next, max_results)
            print(url)
            json_response = TweetSearch.connect_to_endpoint(
                url[0], headers, url[1])

            in_reply_to_user.clear()
            text.clear()
            lang.clear()
            tweet_id.clear()
            source.clear()
            conversation_id.clear()
            reply_settings.clear()
            retweet_count.clear()
            reply_count.clear()
            like_count.clear()
            quote_count.clear()
            author_id.clear()
            created_at.clear()

            loc.clear()
            username.clear()
            verified.clear()

            location.clear()
            ver.clear()
            usern.clear()

            with open('data.json', 'w') as f:
                json.dump(json_response, f)

            f = open("data.json")
            data = json.load(f)
            # print(data)

            time.sleep(1)

            try:
                for tweet in data['data']:
                    print(tweet['conversation_id'],tweet['author_id'])
                    print(tweet)
                    # not there in every tweet
                    if 'in_reply_to_user_id' in tweet.keys():
                        in_reply_to_user.append(tweet['in_reply_to_user_id'])
                    else:
                        in_reply_to_user.append(None)
                    text.append(tweet['text'])
                    lang.append(tweet['lang'])
                    tweet_id.append(tweet['id'])
                    # source.append(tweet['source'])
                    conversation_id.append(tweet['conversation_id'])
                    reply_settings.append(tweet['reply_settings'])

                    retweet_count.append(
                        tweet['public_metrics']['retweet_count'])
                    reply_count.append(tweet['public_metrics']['reply_count'])
                    like_count.append(tweet['public_metrics']['like_count'])
                    quote_count.append(tweet['public_metrics']['quote_count'])
                    author_id.append(tweet["author_id"])
                    created_at.append(tweet["created_at"])

                for i in data["includes"]["users"]:
                    username[i["id"]] = i["username"]
                    verified[i["id"]] = i["verified"]
                    if "location" in i.keys():
                        loc[i["id"]] = i["location"]
                    else:
                        loc[i["id"]] = "NULL"

                for i in author_id:
                    if i in username.keys():
                        usern.append(username[i])
                    else:
                        usern.append("NULL")

                    if i in verified.keys():
                        ver.append(verified[i])
                    else:
                        ver.append("NULL")

                    if i in loc.keys():
                        location.append(loc[i])
                    else:
                        location.append("NULL")
            except:
                pass

                print(len(author_id), len(usern), len(ver), len(created_at), len(text), len(tweet_id), len(source), len(
                    lang), len(like_count), len(reply_count), len(retweet_count), len(quote_count), len(location))
            df2 = pd.DataFrame({"author_id": author_id, "username": usern, "verified": ver, "created_at": created_at, "tweet": text, "tweet_id": tweet_id,
                               "lang": lang, "like_count": like_count, "reply_count": reply_count, "retweet_count": retweet_count, "quote_count": quote_count, "Location": location})
            print(df2)

            output_file = output_file.append(df2, ignore_index=True)
            print(output_file)

        print("Total Tweets: ", len(output_file['tweet']))
        null_count = output_file[output_file['Location'] == "NULL"].count()
        print("Null Location : ", null_count['Location'])
        print("Done")

        return output_file
