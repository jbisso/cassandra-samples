using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using VideoDbApplication.objects;

namespace VideoDbApplication.dao
{
    public interface IVideoDbDAO
    {
        public void Close();
	    public User GetUserByUserNameUsingString(String userName);
    	public User GetUserByUserNameUsingPreparedStatement(String userName);
    	public void SetUserByPreparedStatement(User user);
        public void SetUserByUsingString(User user);
        //public Video getVideoByIdUsingQueryBuilder(String videoId);
    	public List<Video> GetVideosByUserNameUsingAsyncRead(String userName);
	    //public List<Video> GetVideosByTagsUsingAsyncReadThreads(List<String> tags);
        public List<Video> GetVideosByTagUsingAsyncRead(String tag);
        public long GetRatingForVideo(Guid videoId);
        public void SetRatingForVideo(Guid videoId, int rating);
        //public void SetCommentForVideo(Guid videoId, String userName, String comment);
	    public List<Comment> GetCommentsByUserNameUsingPreparedStatement(String userName);
	    public List<Comment> GetCommentsByVideoIdUsingPreparedStatement(Guid videoId);
	    public long GetLastStopEvent(Guid videoId, String userName);
	    public void AddCreditForUser(String userName, int creditValue);
	    public void RemoveCreditForUser(String userName, int creditValue);
    }
}
