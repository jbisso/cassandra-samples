package com.datastax.videodb;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import com.datastax.videodb.dao.VideoDbBasicImpl;
import com.datastax.videodb.object.Comment;
import com.datastax.videodb.object.User;
import com.datastax.videodb.object.Video;

public class VideoDb {

	public static void main(String[] args) {

		// Load our cluster config.
		Properties prop = new Properties();

		try {
			prop.load(VideoDb.class.getResourceAsStream("/cluster.properties"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		ArrayList<String> cassandraNodes = new ArrayList<String>();
		cassandraNodes.add(prop.getProperty("nodes"));

		VideoDbBasicImpl vdb = new VideoDbBasicImpl(cassandraNodes,
				prop.getProperty("keyspace"));

		// Basic get of a user. This is using the string method of executing CQL
		User user = vdb.getUserByUsernameUsingString("tcodd");

		System.out.println("Get user by using CQL string: " + user);

		// Get the same user but with a prepared statement
		user = vdb.getUserByUsernameUsingPreparedStatement("tcodd");

		System.out.println("Get user by using prepared statement: " + user);

		// Add a new user
		User newUser = new User("cdate","Chris","Date", "cdate@relational.com","6cb75f652a9b52798eb6cf2201057c73",Calendar.getInstance().getTime(),0,UUIDs.timeBased());
		
		vdb.setUserByPreparedStatement(newUser);
		
		
		// Get a list of videos. This uses the simple Async Read feature.
		List<Video> videosByTag = vdb.getVideosByTagUsingAsyncRead("lol");
		for (Video video : videosByTag) {
			System.out.println("Video by AsyncRead" + video);

		}

		// Get a list of videos. This uses a threaded Async Read feature.
		// This method will take a list of tags to query.

		List<String> tags = new ArrayList<String>();
		tags.add("lol");
		tags.add("cat");

		List<Video> videosByTagList = vdb
				.getVideosByTagsUsingAsyncReadThreads(tags);
		for (Video video : videosByTagList) {
			System.out.println("Video by AsyncRead Threads" + video);

		}

		// We'll use the videoID to set a rating of 4
		vdb.setRatingForVideo(
				UUID.fromString("99051fe9-6a9c-46c2-b949-38ef78858dd0"), 4);

		// Iterate over the same list and get the overall rating
		for (Video video : videosByTag) {
			System.out.print("Video " + video.getVideoname()
					+ " had an average of ");

			// We'll use the videoID of each to get a rating
			System.out.println(vdb.getRatingForVideo(video.getVideoid()));

		}

		// Set a comment for one video. The underlying method will set on two
		// tables at the same time.
		vdb.setCommentForVideo(
				UUID.fromString("99051fe9-6a9c-46c2-b949-38ef78858dd0"),
				"tcodd", "Worst. Video. Ever.");

		// Get a list of comments by VideoID, a UUID.
		List<Comment> comments = vdb
				.getCommentsByVideoIdUsingPreparedStatement(UUID
						.fromString("99051fe9-6a9c-46c2-b949-38ef78858dd0"));

		for (Comment comment : comments) {
			System.out.println("Get comments by VideoID: " + comment);
		}

		// Get a list of comments by UserName
		comments = vdb.getCommentsByUsernameUsingPreparedStatement("tcodd");

		for (Comment comment : comments) {
			System.out.println("Get comments by UserName: " + comment);
		}

		// Get the last stop event for a video
		long video_timestamp = vdb.getLastStopEvent(
				UUID.fromString("99051fe9-6a9c-46c2-b949-38ef78858dd0"),
				"tcodd");
		
		System.out.println("Video timestamp of last stop event: " + video_timestamp);
		

		// Close our connection and exit. Exit is required since we are running
		// threads.
		vdb.close();
		System.exit(0);
	}

}
