using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using VideoDbApplication.dao;
using VideoDbApplication.objects;
using VideoDbApplication.util;

namespace VideoDbApplication
{
    class Program
    {
        static void Main(string[] args)
        {
            List<String> contactPoints = new List<String>();
            contactPoints.Add("127.0.0.1");
            IVideoDbDAO vdb = new VideoDbBasicImpl(contactPoints, "videodb");
		    // Basic get of a user. This is using the string method of executing CQL
		    User user = vdb.GetUserByUserNameUsingString("tcodd");
		    Console.WriteLine("Get user by using CQL string: " + user);
		    // Get the same user but with a prepared statement
		    user = vdb.GetUserByUserNameUsingPreparedStatement("tcodd");
		    Console.WriteLine("Get user by using prepared statement: " + user);
		    // Add a new user
		    User newUser = new User("cdate", 
                "Chris", 
                "Date", 
                "cdate@relational.com",
                "6cb75f65-2a9b-5279-8eb6-cf2201057c73",
                DateTime.Now,
                0, 
                Guids.GenerateTimeBasedGuid());
		    vdb.SetUserByPreparedStatement(newUser);

		    // Get a list of videos. This uses the simple Async Read feature.
		    List<Video> videosByTag = vdb.GetVideosByTagUsingAsyncRead("lol");
		    foreach (Video video in videosByTag)
            {
			    Console.WriteLine("Video by AsyncRead" + video);
		    }
		    // Get a list of videos. This uses a threaded Async Read feature.
		    // This method will take a list of tags to query.
		    List<String> tags = new List<String>();
		    tags.Add("lol");
		    tags.Add("cat");
		    // We'll use the videoID to set a rating of 4
		    vdb.SetRatingForVideo(new Guid("99051fe9-6a9c-46c2-b949-38ef78858dd0"), 4);
		    // Iterate over the same list and get the overall rating
		    foreach (Video video in videosByTag)
            {
			    Console.Write("Video " + video.VideoName
					    + " had an average of ");
			    // We'll use the videoID of each to get a rating
			    Console.WriteLine(vdb.GetRatingForVideo(video.VideoId));
		    }

            /*
		    List<Video> videosByTagList = vdb.GetVideosByTagsUsingAsyncReadThreads(tags);
		    for (Video video : videosByTagList) {
			    Console.WriteLine("Video by AsyncRead Threads" + video);
		    }

		    // Set a comment for one video. The underlying method will set on two
		    // tables at the same time.
		    vdb.GetCommentForVideo(
				    new Guid("99051fe9-6a9c-46c2-b949-38ef78858dd0"),
				    "tcodd", "Worst. Video. Ever.");
            */

		    // Get a list of comments by VideoID, a UUID.
		    List<Comment> comments = vdb.GetCommentsByVideoIdUsingPreparedStatement(
                new Guid("99051fe9-6a9c-46c2-b949-38ef78858dd0"));

		    foreach (Comment comment in comments)
            {
			    Console.WriteLine("Get comments by VideoID: " + comment);
		    }

		    // Get a list of comments by UserName
		    comments = vdb.GetCommentsByUserNameUsingPreparedStatement("tcodd");
		    foreach (Comment comment in comments)
            {
			    Console.WriteLine("Get comments by UserName: " + comment);
		    }
		    // Get the last stop event for a video
		    long videoTimestamp = vdb.GetLastStopEvent(
				    new Guid("99051fe9-6a9c-46c2-b949-38ef78858dd0"),
				    "tcodd");
            Console.WriteLine("Video timestamp of last stop event: " + videoTimestamp);
		

		    // Close our connection and exit. Exit is required since we are running
		    // threads.
		    vdb.Close();
            return;
        }
    }
}
