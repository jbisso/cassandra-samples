using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VideoDbApplication.objects
{
    class Comment
    {
        public Guid VideoId { set; get; }
        public String UserName { set; get; };
        public DateTimeOffset CommentTS { set; get; }
        public String Comment { set; get; }

	    public String ToString() {
		    return "Comment [VideoId=" + VideoId + ", UserName=" + UserName
				    + ", CommentTS=" + CommentTS + ", Comment=" + Comment + "]";
	    }
    }
}
