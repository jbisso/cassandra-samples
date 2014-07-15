using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Cassandra;

using VideoDbApplication.policy;
using VideoDbApplication.objects;

namespace VideoDbApplication.dao
{
    public class VideoDbBasicImpl : IVideoDbDAO
    {
        private static const String GET_USER_BY_USERNAME = "SELECT * FROM users WHERE username = ?";
        private static const String SET_USER = "INSERT INTO users (username, firstname, lastname, email, password, created_date) VALUES (?,?,?,?,?,?)";
	    private static const String GET_VIDEOS_BY_USERNAME = "SELECT videoid FROM username_video_index WHERE username = ?";
	    private static const String GET_VIDEO_BY_ID = "SELECT * FROM videos WHERE videoid = ?";
	    private static const String GET_VIDEOS_BY_TAG = "SELECT videoid FROM tag_index WHERE tag = ?";
	    private static const String GET_RATING_BY_VIDEO = "SELECT rating_counter, rating_total FROM  video_rating WHERE videoid = ?";
	    private static const String SET_COMMENT_BY_VIDEO = "   INSERT INTO comments_by_video (videoid,username,comment_ts, comment) VALUES (?,?,?,?)";
	    private static const String SET_COMMENT_BY_USERNAME = "   INSERT INTO comments_by_user (videoid,username,comment_ts, comment) VALUES (?,?,?,?)";
	    private static const String GET_COMMENT_BY_VIDEOID = "SELECT videoid,username,comment_ts, comment FROM  comments_by_video WHERE videoid = ?";
        private static const String GET_COMMENT_BY_USERNAME = "SELECT videoid,username,comment_ts, comment FROM  comments_by_user WHERE username = ?";

        public Cluster cluster { set; get; }
	    public ISession session { set; get; }

	    private PreparedStatement getUserByNamePreparedStatement;
	    private PreparedStatement setUser;
	    private PreparedStatement getVideosByUsernamePreparedStatement;
	    private PreparedStatement getVideoByIDPreparedStatement;
	    private PreparedStatement getVideosByTagPreparedStatement;
	    private PreparedStatement getRatingByVideoPreparedStatement;
	    private PreparedStatement setCommentByVideo;
	    private PreparedStatement setCommentByUsername;
	    private PreparedStatement getCommentByUsername;
	    private PreparedStatement getCommentByVideoId;

        private TimeOfDayRetryPolicy timeOfDayRetryPolicy;
	    //private ExecutorService executor;

        public VideoDbBasicImpl(List<String> contactPoints, String keyspace) 
        {
		    cluster = Cluster
			        .Builder()
			        .AddContactPoints(contactPoints.ToArray())
			        .WithRetryPolicy(Policies.DefaultRetryPolicy)
			        .Build();

		    session = cluster.Connect(keyspace);

		    getUserByNamePreparedStatement = session.Prepare(GET_USER_BY_USERNAME);
		    setUser = session.Prepare(SET_USER);
		    getVideosByUsernamePreparedStatement = session
				    .Prepare(GET_VIDEOS_BY_USERNAME);
            getVideoByIDPreparedStatement = session.Prepare(GET_VIDEO_BY_ID);
            getVideosByTagPreparedStatement = session.Prepare(GET_VIDEOS_BY_TAG);
		    getRatingByVideoPreparedStatement = session
                    .Prepare(GET_RATING_BY_VIDEO);
            setCommentByVideo = session.Prepare(SET_COMMENT_BY_VIDEO);
            setCommentByUsername = session.Prepare(SET_COMMENT_BY_USERNAME);
            getCommentByUsername = session.Prepare(GET_COMMENT_BY_USERNAME);
            getCommentByVideoId = session.Prepare(GET_COMMENT_BY_VIDEOID);

		    timeOfDayRetryPolicy = new TimeOfDayRetryPolicy(9, 17);

		    // Create a thread pool equal to the number of cores
            /*
		    executor = Executors.newFixedThreadPool(Runtime.getRuntime()
				    .availableProcessors());
             */
	    }

        public void Close()
        {
            cluster.Shutdown();
        }

	    public User GetUserByUserNameUsingString(String userName)
        {
		    User user = new User();
		    RowSet rows = session.Execute("SELECT * FROM users WHERE username = '"
				    + userName + "'");
		    foreach ( Row row in rows.GetRows() )
            {
                user.UserName = userName;
			    user.FirstName = row.GetValue<String>("firstname");
			    user.LastName = row.GetValue<String>("lastname");
			    user.Email = row.GetValue<String>("email");
			    user.Password = row.GetValue<String>("Password");
			    user.CreatedDate = row.GetValue<DateTimeOffset>("created_date");
		    }
		    return user;    
        }

    	public User GetUserByUserNameUsingPreparedStatement(String userName)
        {
            User user = new User();
		    // The BoundStatement is created by the PreparedStatement created above
		    BoundStatement boundStatement = getUserByNamePreparedStatement.Bind(userName);
		    // Custom retry policy
		    boundStatement.SetRetryPolicy(timeOfDayRetryPolicy);
		    RowSet rows = session.Execute(boundStatement);
		    foreach (Row row in rows.GetRows()) 
            {
                user.UserName = userName;
			    user.FirstName = row.GetValue<String>("firstname");
			    user.LastName = row.GetValue<String>("lastname");
			    user.Email = row.GetValue<String>("email");
			    user.Password = row.GetValue<String>("Password");
			    user.CreatedDate = row.GetValue<DateTimeOffset>("created_date");
		    }
		    return user;
        }

    	public void SetUserByPreparedStatement(User user)
        {
            BoundStatement boundStatement = setUser.Bind(
                user.UserName, user.FirstName, user.LastName, user.Email,
                user.Password, user.CreatedDate);
		    session.Execute(boundStatement);
        }

        public void SetUserByUsingString(User user)
        {
            StringBuilder userInsert = new StringBuilder(
				"INSERT INTO users (username, firstname, lastname, email, password, created_date) VALUES (");
            userInsert.Append("'");
		    userInsert.Append(user.UserName);
            userInsert.Append("', '");
		    userInsert.Append(user.FirstName);
            userInsert.Append("', '");
		    userInsert.Append(user.LastName);
            userInsert.Append("', '");
		    userInsert.Append(user.Email);
            userInsert.Append("', '");
		    userInsert.Append(user.Password);
            userInsert.Append("', '");
		    userInsert.Append(user.CreatedDate.ToString());
            userInsert.Append("'");
		    userInsert.Append(")");
		    session.Execute(userInsert.ToString());
        }

        //public Video getVideoByIdUsingQueryBuilder(String videoId) { }

    	public List<Video> GetVideosByUserNameUsingAsyncRead(String userName)
        {
            // Get a list of videoIds for one user from username
		    BoundStatement boundStatement = getVideosByUsernamePreparedStatement.Bind(userName);
		    // We'll create a List of futures for each query we need to run.
		    List<IAsyncResult> futures = new List<IAsyncResult>();
		    List<Video> videos = new List<Video>();
		    // First we will grab a list of videoIds
		    foreach ( Row row in session.Execute(boundStatement).GetRows() )
            {
			    // For each videoId we will create a query to get each Video
			    // As each is created, they are executed in the background
			    futures.Add(session.BeginExecute(getVideoByIDPreparedStatement
					    .Bind(row.GetValue<Guid>("videoid")), null, null));
		    }

		    foreach (IAsyncResult future in futures)
            {
			    // Wait for this IAsyncResult to finish
                // is used when we don't mind the
			    // threads being interrupted. The option is to just use
			    // get and catch the exception

                future.AsyncWaitHandle.WaitOne();
			    foreach (Row row in session.EndExecute(future).GetRows() )
                {
				    Video video = new Video();
				    video.VideoId = row.GetValue<Guid>("videoid");
				    video.VideoName = row.GetValue<String>("videoName");
				    video.UserName = row.GetValue<String>("username");
				    video.Description = row.GetValue<String>("description");
				    video.Tags = row.GetValue<List<String>>("tags").ToString();
				    video.UploadDate = row.GetValue<DateTimeOffset>("upload_date").ToString();
				    videos.Add(video);
			    }
		    }
		    return videos;
        }

	    //public List<Video> GetVideosByTagsUsingAsyncReadThreads(List<String> tags) { }

        public List<Video> GetVideosByTagUsingAsyncRead(String tag) 
        {
		    // Get a list of videoIds for one user from username
		    BoundStatement boundStatement = getVideosByTagPreparedStatement.Bind(tag);
		    List<IAsyncResult> futures = new List<IAsyncResult>();
		    List<Video> videos = new List<Video>();
		    foreach ( Row row in session.Execute(boundStatement).GetRows() )
            {
			    futures.Add(session.BeginExecute(getVideoByIDPreparedStatement
					    .Bind(row.GetValue<Guid>("videoid")), null, null));
		    }
		    foreach (IAsyncResult future in futures) {
			    // Uninterrupted get
                future.AsyncWaitHandle.WaitOne();
			    foreach ( Row row in session.EndExecute(future).GetRows() ) {
				    Video video = new Video();
				    video.VideoId = row.GetValue<Guid>("videoid");
				    video.VideoName = row.GetValue<String>("videoName");
				    video.UserName = row.GetValue<String>("username");
				    video.Description = row.GetValue<String>("description");
				    video.Tags = row.GetValue<List<String>>("tags").ToString();
				    video.UploadDate = row.GetValue<DateTimeOffset>("upload_date").ToString();
				    videos.Add(video);
			    }
		    }
		    return videos;
        }

        public long GetRatingForVideo(Guid videoId)
        {
            BoundStatement boundStatement = getRatingByVideoPreparedStatement.Bind(videoId);
		    RowSet rows = session.Execute(boundStatement);
		    Row row = rows.GetRows().First();
		    if ( row == null ) {
			    return 0;
		    }
		    // Get the count and total rating for the video
		    long total = row.GetValue<long>("rating_total");
		    long count = row.GetValue<long>("rating_counter");
		    // Divide the total by the count and return an average
		    return (total / count);
        }

        public void SetRatingForVideo(Guid videoId, int rating)
        {
            // Very simple way of incrementing a count
		    session.Execute("UPDATE video_rating SET rating_counter = rating_counter + 1, rating_total = rating_total + "
				    + rating + " WHERE videoid = " + videoId);
        }

        //public void SetCommentForVideo(Guid videoId, String userName, String comment) { }

	    public List<Comment> GetCommentsByUserNameUsingPreparedStatement(String userName)
        {
		    BoundStatement boundStatement = getCommentByUsername.Bind(userName);
		    List<Comment> comments = new List<Comment>();
		    foreach (Row row in session.Execute(boundStatement).GetRows())
            {
			    Comment comment = new Comment();
			    comment.VideoId = row.GetValue<Guid>("videoId");
			    comment.UserName = row.GetValue<String>("username");
			    comment.CommentTS = row.GetValue<DateTimeOffset>("comment_ts");
			    comment.Comment = row.GetValue<String>("comment");
			    comments.Add(comment);
		    }
		    return comments;
        }

	    public List<Comment> GetCommentsByVideoIdUsingPreparedStatement(Guid videoId)
        {
		    BoundStatement boundStatement = getCommentByVideoId.Bind(videoId);
		    List<Comment> comments = new List<Comment>();
		    foreach (Row row in session.Execute(boundStatement).GetRows())
            {
			    Comment comment = new Comment();
			    comment.VideoId = row.GetValue<Guid>("videoId");
			    comment.UserName = row.GetValue<String>("username");
			    comment.CommentTS = row.GetValue<DateTimeOffset>("comment_ts");
			    comment.Comment = row.GetValue<String>("comment");
			    comments.Add(comment);
		    }

		    return comments;
        }

	    public long GetLastStopEvent(Guid videoId, String userName)
        {
		    long videoTimestamp = 0;
		    // Get a list of the last 5 events to find the last stop
            Statement statement = new SimpleStatement(
                "SELECT event, video_timestamp FROM videodb.video_event " +
                "WHERE videoId = " + videoId + " AND username = " + userName +
                "LIMIT 5;");
		    RowSet rows = session.Execute(statement);
		    foreach (Row row in rows.GetRows())
            {
			    // Find the first stop event, store it and stop the loop
			    if ( row.GetValue<String>("event").ToLower().Equals("stop") )
                {
				    videoTimestamp = row.GetValue<long>("video_timestamp");
				    break;
			    }
		    }
		    return videoTimestamp;
        }

	    //public void AddCreditForUser(String userName, int creditValue) { }

	    //public void RemoveCreditForUser(String userName, int creditValue) { }
    }
}
