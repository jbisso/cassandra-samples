package com.datastax.videodb.object;

import java.util.Date;
import java.util.UUID;

public class Comment {

	UUID videoid;
	String username;
	Date comment_ts;
	String comment;

	public Comment() {
		
	}

	public UUID getVideoid() {
		return videoid;
	}

	public void setVideoid(UUID videoid) {
		this.videoid = videoid;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public Date getComment_ts() {
		return comment_ts;
	}

	public void setComment_ts(Date comment_ts) {
		this.comment_ts = comment_ts;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	@Override
	public String toString() {
		return "Comment [videoid=" + videoid + ", username=" + username
				+ ", comment_ts=" + comment_ts + ", comment=" + comment + "]";
	}

}
