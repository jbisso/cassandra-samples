#videodb-tutorial

This uses the [DataStax Java driver](https://github.com/datastax/java-driver) with a CQL3 application example.

##Data Model

The following tables are created. You can use [this cql file](https://github.com/jbisso/cassandra-samples/tree/master/java/videodb-tutorial/src/main/resources/videodb.cql) to import in your own database.

- users
- videos

- username_video_index
- tag_index
- comments_by_video
- comments_by_user

- video_rating
- video_event
- credit_transaction

To insert data you will then need [this file](https://github.com/jbisso/ccassandra-samples/tree/master/java/videodb-tutorial/src/main/resources/videodb_dummy_data.cql) to build some sample content. 


## Using the DataStax java driver with this example

The following methods are implemented.

- **getUserByUsernameUsingString**: Basic select using a full command string. 
- **getUserByUsernameUsingPreparedStatement**: Example of prepared statement for same select. 
- **getUserByUsernameUsingQueryBuilder**: Query builder for same select as string. 
- **getVideoByIdUsingQueryBuilder**
- **getVideosByUsernameUsingAsyncRead**: Uses username_video_index to find all videos for a user. Performs a basic AsyncRead
- **getRatingForVideo**: Selects counter values, does the math an returns the average
- **setRatingForVideo**: Adds a rating to a video. Uses counters.
- **getVideosByTagsUsingAsyncReadThreads**: Uses username_video_index to find all videos for a user. Concurrent threads executing an AsyncRead
- **getVideosByTagUsingAsyncRead**
- **setCommentForVideo**: Set a comment. Demonstrate setting both tables with a batch.
- **getCommentsByUsernameUsingPreparedStatement**: Grab a list of comments from a username perspective
- **getCommentsByVideoIdUsingPreparedStatement**: Grab a list of comments from a Video ID perspective
- **getLastStopEvent**: Query the video event table and find the last stop event for a video and user
- **addCreditForUser**: Demonstrate how to simulate a transaction for adding a credit to a user account
- **removeCreditForUser**: Now remove a credit using the same type of transaction type semantics

