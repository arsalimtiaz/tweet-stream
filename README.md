# tweet-stream

Pipeline to stream, preprocess and store twitter data in .csv and .pkl format

1. Clone the repository.
2. Create an access_key.yaml file in the root directory with the following format:

   ```
   Bearer Token: `<YOUR BEARER TOKEN>`
   ```
3. Make changes to the config.yaml based on the timeline of tweets that you require:

   ```
   # the starting timestamp of your serach
   start_time: "2023-01-31T00:00:00.000Z"  

   # the last timestamp of your search
   end_time: "2023-01-31T23:00:00.000Z"

   # the time bracket that you want to make requests and search
   time_delta:
     seconds: 0
     minutes: 0
     hours: 1
     days: 0

   # Search query based on Twitter API intructions
   query: "lang:en -is:retweet place_country:US (Twitter API)"

   # Maximum tweets you want at each time bracket
   max_results: 10

   ```
4. run `python main.py`
