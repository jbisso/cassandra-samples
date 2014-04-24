package com.datastax.videodb.dao;

import java.util.List;
import java.util.UUID;

import com.datastax.videodb.pojo.Comment;
import com.datastax.videodb.pojo.User;
import com.datastax.videodb.pojo.Video;


public interface VideoDbDAO {
    public abstract User getUserByUserNameUsingString(String userName);
    public abstract User getUserByUserNameUsingPreparedStatement(String userName);
    public abstract void setUserByPreparedStatement(User user);
    public abstract void setUserByUsingString(User user);
    public abstract Video getVideoByIdUsingQueryBuilder(String videoId);
    public abstract List<Video> getVideosByUsernameUsingAsyncRead(String userName);
    public abstract List<Video> getVideosByTagsUsingAsyncReadThreads(List<String> tags);
    public abstract List<Video> getVideosByTagUsingAsyncRead(String tag);
    public abstract long getRatingForVideo(UUID videoId);
    public abstract void setRatingForVideo(UUID videoId, int rating);
    public abstract void setCommentForVideo(UUID videoId, String userName, String comment);
    public abstract List<Comment> getCommentsByUserNameUsingPreparedStatement(String userName);
    public abstract List<Comment> getCommentsByVideoIdUsingPreparedStatement(UUID videoId);
    public abstract long getLastStopEvent(UUID videoId, String userName);
    public abstract void addCreditForUser(String userName, int creditValue);
    public abstract void removeCreditForUser(String userName, int creditValue) ;
}
