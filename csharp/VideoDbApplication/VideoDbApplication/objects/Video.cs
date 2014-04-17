using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VideoDbApplication.objects
{
    public class Video {
        public Guid VideoId { set; get; }
        public String VideoName { set; get; }
        public String UserName { set; get; }
        public String Description { set; get; }
        public String Tags { set; get; }
        public String UploadDate { set; get; }

	    public String ToString() {
		    return "Video [VideoId=" + VideoId + ", VideoName=" + VideoName
                    + ", UserName=" + UserName + ", Description=" + Description
                    + ", Tags=" + Tags + ", UploadDate=" + UploadDate + "]";
	    }
    }   
}
