package com.datastax.videodb.object;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class Video {

	private UUID videoid;
	private String videoname;
	private String username;
	private String description;
	private List<String> tags;
	private Date upload_date;

	public UUID getVideoid() {
		return videoid;
	}

	public void setVideoid(UUID videoid) {
		this.videoid = videoid;
	}

	public String getVideoname() {
		return videoname;
	}

	public void setVideoname(String videoname) {
		this.videoname = videoname;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public Date getUpload_date() {
		return upload_date;
	}

	public void setUpload_date(Date upload_date) {
		this.upload_date = upload_date;
	}

	public String toString() {
		return "Video [videoid=" + videoid + ", videoname=" + videoname
				+ ", username=" + username + ", description=" + description
				+ ", tags=" + tags + ", upload_date=" + upload_date + "]";
	}

}
