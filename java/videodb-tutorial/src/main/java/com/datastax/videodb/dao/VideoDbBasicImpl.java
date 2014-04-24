package com.datastax.videodb.dao;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.videodb.pojo.Comment;
import com.datastax.videodb.pojo.User;
import com.datastax.videodb.pojo.Video;
import com.datastax.videodb.policy.TimeOfDayRetryPolicy;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

// Async read - Get and index then multiget
// Load balancing policy

public class VideoDbBasicImpl implements VideoDbDAO {

	private static final Object PRESENT = new Object();

	private static final String GET_USER_BY_USERNAME = "SELECT * FROM users WHERE username = ?";
	private static final String SET_USER = "INSERT INTO users (username, firstname, lastname, email, password, created_date) VALUES (?, ?, ?, ?, ?, ?)";
	private static final String GET_VIDEOS_BY_USERNAME = "SELECT videoid FROM username_video_index WHERE username = ?";
	private static final String GET_VIDEO_BY_ID = "SELECT * FROM videos WHERE videoid = ?";
	private static final String GET_VIDEOS_BY_TAG = "SELECT videoid FROM tag_index WHERE tag = ?";
	private static final String GET_RATING_BY_VIDEO = "SELECT rating_counter, rating_total FROM  video_rating WHERE videoid = ?";
	private static final String SET_COMMENT_BY_VIDEO = "   INSERT INTO comments_by_video (videoid,username,comment_ts, comment) VALUES (?,?,?,?)";
	private static final String SET_COMMENT_BY_USERNAME = "   INSERT INTO comments_by_user (videoid,username,comment_ts, comment) VALUES (?,?,?,?)";
	private static final String GET_COMMENT_BY_VIDEOID = "SELECT videoid, username, comment_ts, comment FROM comments_by_video WHERE videoid = ?";
	private static final String GET_COMMENT_BY_USERNAME = "SELECT videoid, username, comment_ts, comment FROM comments_by_user WHERE username = ?";

	private final Cluster cluster;
	private final Session session;

	private final PreparedStatement getUserByNamePreparedStatement;
	private final PreparedStatement setUser;
	private final PreparedStatement getVideosByUsernamePreparedStatement;
	private final PreparedStatement getVideoByIDPreparedStatement;
	private final PreparedStatement getVideosByTagPreparedStatement;
	private final PreparedStatement getRatingByVideoPreparedStatement;
	private final PreparedStatement setCommentByVideo;
	private final PreparedStatement setCommentByUsername;
	private final PreparedStatement getCommentByUsername;
	private final PreparedStatement getCommentByVideoId;

	private final TimeOfDayRetryPolicy timeOfDayRetryPolicy;

	private final ExecutorService executor;

	public VideoDbBasicImpl(List<String> contactPoints, String keyspace) {
		cluster = Cluster
				.builder()
				.addContactPoints(
						contactPoints.toArray(new String[contactPoints.size()]))
				.withRetryPolicy(Policies.defaultRetryPolicy())
				.build();

		session = cluster.connect(keyspace);

		getUserByNamePreparedStatement = session.prepare(GET_USER_BY_USERNAME);
		setUser = session.prepare(SET_USER);
		getVideosByUsernamePreparedStatement = session
				.prepare(GET_VIDEOS_BY_USERNAME);
		getVideoByIDPreparedStatement = session.prepare(GET_VIDEO_BY_ID);
		getVideosByTagPreparedStatement = session.prepare(GET_VIDEOS_BY_TAG);
		getRatingByVideoPreparedStatement = session
				.prepare(GET_RATING_BY_VIDEO);
		setCommentByVideo = session.prepare(SET_COMMENT_BY_VIDEO);
		setCommentByUsername = session.prepare(SET_COMMENT_BY_USERNAME);
		getCommentByUsername = session.prepare(GET_COMMENT_BY_USERNAME);
		getCommentByVideoId = session.prepare(GET_COMMENT_BY_VIDEOID);

		timeOfDayRetryPolicy = new TimeOfDayRetryPolicy(9, 17);

		// Create a thread pool equal to the number of cores
		executor = Executors.newFixedThreadPool(Runtime.getRuntime()
				.availableProcessors());
	}

	public void close() {
		cluster.close();
	}

	/**
	 * Least secure method of executing CQL but here as an example.
	 * @param username
	 * @return the user
	 */
	public User getUserByUserNameUsingString(String userName) {
		User user = null;

		ResultSet rs = session.execute("SELECT * FROM users WHERE username = '"
				+ userName + "'");

		// A result set has Rows which can be iterated over
		for (Row row : rs) {
			user.setUserName(userName);
			user.setFirstName(row.getString("firstname"));
			user.setLastName(row.getString("lastname"));
			user.setEmail(row.getString("email"));
			user.setPassword(row.getString("Password"));
			user.setCreatedDate(row.getDate("created_date"));
		}
		return user;
	}

	/**
	 * Simple example using a Prepared Statement.
	 * @param username
	 * @return the user
	 */
	public User getUserByUserNameUsingPreparedStatement(String userName) {
		User user = null;

		// The BoundStatement is created by the PreparedStatement created above
		BoundStatement boundStatement = getUserByNamePreparedStatement.bind();
		boundStatement.setString("username", userName);

		// Custom retry policy
		boundStatement.setRetryPolicy(timeOfDayRetryPolicy);

		ResultSet results = session.execute(boundStatement);
		// can only be one row when queried by primary key column
        Row row = results.one();
		if ( row != null ) {
		    user = new User();
			user.setUserName(userName);
			user.setFirstName(row.getString("firstname"));
			user.setLastName(row.getString("lastname"));
			user.setEmail(row.getString("email"));
			user.setPassword(row.getString("password"));
			user.setCreatedDate(row.getDate("created_date"));
		}

		return user;
	}

	/**
	 * Inserts a User by prepared statement. Sets the following fields:
	 * username, firstname, lastname, email, password, created_date
	 * @param user
	 */
	public void setUserByPreparedStatement(User user) {
		BoundStatement bs = setUser.bind();
		bs.setString("username", user.getUserName());
		bs.setString("firstname", user.getFirstName());
		bs.setString("lastname", user.getLastName());
		bs.setString("email", user.getEmail());
		bs.setString("password", user.getPassword());
		bs.setDate("created_date", user.getCreatedDate());

		session.execute(bs);
	}

	/**
	 * Inserts a User by prepared statement. Sets the following fields:
	 * username, firstname, lastname, email, password, created_date
	 * @param user
	 */
	public void setUserByUsingString(User user) {
		StringBuffer userInsert = new StringBuffer(
				"INSERT INTO users (username, firstname, ");
		userInsert.append("lastname, email, password, created_date) VALUES (");
		userInsert.append("'");
		userInsert.append(user.getUserName());
		userInsert.append("', '");
		userInsert.append(user.getFirstName());
		userInsert.append("', '");
		userInsert.append(user.getLastName());
		userInsert.append("', '");
		userInsert.append(user.getEmail());
		userInsert.append("', '");
		userInsert.append(user.getPassword());
		userInsert.append("', '");
		userInsert.append(user.getCreatedDate().toString());
		userInsert.append("'");
		userInsert.append(")");

		session.execute(userInsert.toString());
	}

	/**
	 * Using QueryBuilder is the most secure way of creating a CQL Statement.
	 * Avoids issues with injection style attacks.
	 * @param videoId
	 * @return
	 */
	public Video getVideoByIdUsingQueryBuilder(String videoId) {
		Video video = new Video();

		Statement query = select().all().from("videodb", "videos")
				.where(eq("videoId", videoId)).limit(10);

		// We can change our Consistency Level on the fly, per read or write.
		query.setConsistencyLevel(ConsistencyLevel.ONE);

		// We can also enable tracing on this statement
		query.enableTracing();

		ResultSet rs = session.execute(query);

		// If you want the trace information, you can use the following method
		// QueryTrace queryTrace = rs.getExecutionInfo().getQueryTrace();

		for (Row row : rs) {
			video.setVideoid(row.getUUID("videoid"));
			video.setVideoname(row.getString("videoName"));
			video.setUsername(row.getString("username"));
			video.setDescription(row.getString("description"));

			// This is a get of a CQL List collection. You must specify
			// the type of the list. In this case String for varchar
			video.setTags(row.getList("tags", String.class));
			video.setUpload_date(row.getDate("upload_date"));
		}
		return video;

	}

	/**
	 * A powerful and efficient way of reading data from your cluster. When you
	 * need more than one row, consider using an AsyncRead with a
	 * ResultSetFuture
	 * Each query is done in parallel and is non-blocking until you issue the get.

	 * @param username
	 * @return
	 */
	public List<Video> getVideosByUsernameUsingAsyncRead(String userName) {
		// Get a list of videoIds for one user from userName
		BoundStatement bs = getVideosByUsernamePreparedStatement.bind();

		// We'll create a List of futures for each query we need to run.
		List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();
		List<Video> videos = new ArrayList<Video>();

		bs.setString("username", userName);

		// First we will grab a list of videoIds
		for (Row row : session.execute(bs)) {
			// For each videoId we will create a query to get each Video
			// As each is created, they are executed in the background
			futures.add(session.executeAsync(getVideoByIDPreparedStatement
					.bind(row.getUUID("videoid"))));
		}

		for (ResultSetFuture future : futures) {
			// getUninterruptibly() is used when we don't mind the
			// threads being interrupted. The option is to just use
			// get and catch the exception
			for (Row row : future.getUninterruptibly()) {
				Video video = new Video();
				video.setVideoid(row.getUUID("videoid"));
				video.setVideoname(row.getString("videoName"));
				video.setUsername(row.getString("username"));
				video.setDescription(row.getString("description"));
				video.setTags(row.getList("tags", String.class));
				video.setUpload_date(row.getDate("upload_date"));

				videos.add(video);
			}
		}
		return videos;
	}

	/**
	 * This is a bit more of a complicated example using AsyncRead and threads.
	 * The threads are used to background a query and notify when complete. After
	 * all queries are run, we use a latch wait for the last result to be returned.
	 * @param tags
	 * @return
	 */
	public List<Video> getVideosByTagsUsingAsyncReadThreads(List<String> tags) {
		List<Video> videos = new ArrayList<Video>();

		// ArrayList is not thread safe so not good for concurrent
		// In this case we will ensure it is synchronized
		final List<ResultSetFuture> videoFutures = Collections
				.synchronizedList(new ArrayList<ResultSetFuture>());

		// Latch for holding until the last query is returned
		final CountDownLatch latch = new CountDownLatch(tags.size());

		// ConcurrentHashMap will be used to create a unique list of videoIds
		final ConcurrentHashMap<UUID, Object> videoIds = new ConcurrentHashMap<UUID, Object>();

		for (String tag : tags) {
			BoundStatement bs = getVideosByTagPreparedStatement.bind(tag);
			final ResultSetFuture future = session.executeAsync(bs);

			// For each videoId in the tag table, we create a background thread
			// to retrieve the video details. Those futures are put in a hash to
			// eliminate
			// duplicates in our final list.
			future.addListener(new Runnable() {

				@Override
				public void run() {
					for (Row row : future.getUninterruptibly()) {
						UUID videoId = row.getUUID("videoid");

						// Create a non-duplicate hash map of video IDs
						if (videoIds.putIfAbsent(videoId, PRESENT) == null) {

							videoFutures.add(session
									.executeAsync(getVideoByIDPreparedStatement
											.bind(videoId)));
						}
					}
					latch.countDown();
				}
			}, executor);
		}

		// Wait for each thread to finish
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		for (ResultSetFuture future : videoFutures) {
			// For each future we now will grab the result set of each video
			for (Row row : future.getUninterruptibly()) {
				Video video = new Video();
				video.setVideoid(row.getUUID("videoid"));
				video.setVideoname(row.getString("videoName"));
				video.setUsername(row.getString("username"));
				video.setDescription(row.getString("description"));
				video.setTags(row.getList("tags", String.class));
				video.setUpload_date(row.getDate("upload_date"));

				videos.add(video);
			}
		}
		return videos;
	}

	public List<Video> getVideosByTagUsingAsyncRead(String tag) {

		// Get a list of videoIds for one user from userName
		BoundStatement bs = getVideosByTagPreparedStatement.bind(tag);
		List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();
		List<Video> videos = new ArrayList<Video>();

		for (Row row : session.execute(bs)) {
			futures.add(session.executeAsync(getVideoByIDPreparedStatement
					.bind(row.getUUID("videoid"))));
		}

		for (ResultSetFuture future : futures) {
			// Uninterrupted get
			for (Row row : future.getUninterruptibly()) {
				Video video = new Video();
				video.setVideoid(row.getUUID("videoid"));
				video.setVideoname(row.getString("videoName"));
				video.setUsername(row.getString("username"));
				video.setDescription(row.getString("description"));
				video.setTags(row.getList("tags", String.class));
				video.setUpload_date(row.getDate("upload_date"));

				videos.add(video);
			}
		}
		return videos;
	}

	public long getRatingForVideo(UUID videoId) {
		BoundStatement bs = getRatingByVideoPreparedStatement.bind(videoId);

		ResultSet rs = session.execute(bs);

		Row row = rs.one();

		if ( row == null ) {
			return 0;
		}
		// Get the count and total rating for the video
		long total = row.getLong("rating_total");
		long count = row.getLong("rating_counter");

		// Divide the total by the count and return an average
		return (total / count);
	}

	public void setRatingForVideo(UUID videoId, int rating) {
		// Very simple way of incrementing a count
		session.execute("UPDATE video_rating SET rating_counter = rating_counter + 1, rating_total = rating_total + "
				+ rating + " WHERE videoid = " + videoId);
	}

	// To set a comment, we'll use a batch to put in both comment tables at the
	// same time.
	public void setCommentForVideo(UUID videoId, String userName, String comment) {
		Date currentTimestamp = Calendar.getInstance().getTime();

		// The Batch is created by adding multiple statements in order.
		// Each statement can be something other than a insert. Select
		// Update and Delete work in the same batch.
		Batch batch = batch().add(
				insertInto("comments_by_video").value("videoId", videoId)
						.value("username", userName).value("comment", comment)
						.value("comment_ts", currentTimestamp)).add(
				insertInto("comments_by_user").value("videoId", videoId)
						.value("username", userName).value("comment", comment)
						.value("comment_ts", currentTimestamp));

		session.execute(batch);

	}

	public List<Comment> getCommentsByUserNameUsingPreparedStatement(
			String username) {
		BoundStatement bs = getCommentByUsername.bind(username);

		List<Comment> comments = new ArrayList<Comment>();

		for (Row row : session.execute(bs)) {
			Comment comment = new Comment();
			comment.setVideoid(row.getUUID("videoId"));
			comment.setUsername(row.getString("username"));
			comment.setComment_ts(row.getDate("comment_ts"));
			comment.setComment(row.getString("comment"));

			comments.add(comment);
		}
		return comments;
	}

	public List<Comment> getCommentsByVideoIdUsingPreparedStatement(UUID videoId) {
		BoundStatement bs = getCommentByVideoId.bind(videoId);
		List<Comment> comments = new ArrayList<Comment>();

		for (Row row : session.execute(bs)) {
			Comment comment = new Comment();
			comment.setVideoid(row.getUUID("videoId"));
			comment.setUsername(row.getString("username"));
			comment.setComment_ts(row.getDate("comment_ts"));
			comment.setComment(row.getString("comment"));

			comments.add(comment);
		}

		return comments;
	}

	public long getLastStopEvent(UUID videoId, String username) {
		long video_timestamp = 0;

		// Get a list of the last 5 events to find the last stop
		Statement query = select().column("event").column("video_timestamp")
				.from("videodb", "video_event").where(eq("videoId", videoId))
				.and(eq("username", username)).limit(5);
		session.execute(query);

		for (Row row : session.execute(query)) {
			// Find the first stop event, store it and stop the loop
			if (row.getString("event").equalsIgnoreCase("stop")) {
				video_timestamp = row.getLong("video_timestamp");
				break;
			}
		}

		return video_timestamp;
	}

	// TODO simulated transaction
	public void addCreditForUser(String userName, int creditValue) {
	}

	// TODO simulated transaction
	public void removeCreditForUser(String userName, int creditValue) {
	}
}
