package controllers;

import play.data.DynamicForm;
import play.data.Form;
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
            response().setContentType("text/html");
            result = ok("<h1>User " + byUserName + " not found.</h1>");
        }
        return result;
    }

    // e.g., http://localhost:9000/hello?firstname=Yann&lastname=Clacquot
    
    public static Result hello() {
        DynamicForm requestData = Form.form().bindFromRequest();
        String firstname = requestData.get("firstname");
        String lastname = requestData.get("lastname");
        return ok("Hello " + firstname + " " + lastname);
    }
    
    public static Result submit() {
        return ok("OK");
    }
}
