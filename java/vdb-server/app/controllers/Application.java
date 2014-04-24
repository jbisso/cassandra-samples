package controllers;

import play.mvc.Controller;
import play.mvc.Result;

import views.html.index;
import views.html.user;

import java.util.ArrayList;
import java.util.List;

import com.datastax.videodb.dao.VideoDbBasicImpl;
import com.datastax.videodb.dao.VideoDbDAO;
import com.datastax.videodb.pojo.User;

public class Application extends Controller {
    private static VideoDbDAO dao;

    static {
        List<String> nodes = new ArrayList<String>();
        nodes.add("127.0.0.1");
        dao = new VideoDbBasicImpl(nodes, "videodb");
    }
    
    public static Result index() {
        return ok(index.render("Got request " + request()));
    }
    
    public static Result getUser(String byUserName) {
        Result result = null;
        User theUser = dao.getUserByUserNameUsingPreparedStatement(byUserName);
        if ( theUser != null ) {
            result = ok(user.render(theUser));
        } else {
            result = ok("No user found.");
        }
        return result;
    }

}
