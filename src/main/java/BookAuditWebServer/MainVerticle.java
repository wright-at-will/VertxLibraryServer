package BookAuditWebServer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.Cookie;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainVerticle extends AbstractVerticle {

  String url = "jdbc:mysql://easel2.fulgentcorp.com/yby805" +
    "?" +
    "db=yby805" +
    "&useLegacyDatetimeCode=false&useTimezone=true&serverTimezone=GMT ";
  private MySQLPool client;

  private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

  @Override
  public void start(Promise<Void> promise) throws Exception {
    Future<Void> steps = connectDB().compose(v -> startHttpServer());
    steps.setHandler(promise);
  }

  private Future<Void> connectDB(){
    Promise<Void> promise = Promise.promise();
    MySQLConnectOptions connectOptions = new MySQLConnectOptions()
      .setPort(3306)
      .setHost("easel2.fulgentcorp.com")
      .setDatabase("yby805")
      .setUser("yby805")
      .setPassword("y9X8yYS2ZsFsuK1Xlzgj");
    PoolOptions poolOptions = new PoolOptions();
    client = MySQLPool.pool(vertx,connectOptions,poolOptions);
    if(client != null) {
      LOGGER.info("Client opened");
    } else {
      LOGGER.info("Client Failed to open");
    }

      client.getConnection(ar -> {
      if (ar.failed()) {
        LOGGER.error("Could not open a database connection", ar.cause());
        promise.fail(ar.cause());
      } else {
        SqlConnection connection = ar.result();
        if (connection == null)
          LOGGER.info("The connection could not be created");
        else
          connection.close();
        promise.complete();
      }
    });
    return promise.future();
  }

  private Future<Void> startHttpServer() {
    Promise<Void> promise = Promise.promise();
    HttpServer server = vertx.createHttpServer();
    Router router = Router.router(vertx);
    router.route("/").handler(this::login);
    router.route("/login").handler(this::login);
    router.route("/test").handler(this::test);

    server.requestHandler(router)
      .listen(8888, http -> {
      if (http.succeeded()) {
        promise.complete();
        System.out.println("HTTP server started on port 8888");
      } else {
        promise.fail(http.cause());
      }
    });
    return promise.future();
  }

  private void login(RoutingContext context){
    LOGGER.info("I am sending a cookie back");
    context.response().addCookie(Cookie.cookie("MyCookie","10").setMaxAge(60000000));
    context.response().end();
    //context.response().setChunked(true);
    //context.response().putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
    //context.response().putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS, "GET");
    //context.response().write("Cookie Stamped -> " + name + " : " +value);
    //context.response().end();
    //return context.addCookie(Cookie.cookie("NO!!MyCookie","0"));//context.user().setAuthProvider();
  }

  private void test(RoutingContext context){
    LOGGER.info("Do they have a cookie?");
    Cookie cookie;
    try {
      LOGGER.error(context.request().cookieCount()+" cookie");
      context.response().setChunked(true);
      for(String cookieName:context.request().cookieMap().keySet()){
        LOGGER.info(context.request().getCookie(cookieName).getName()+"From key: "+cookieName);
      }
      if ((cookie = context.request().getCookie("MyCookie")) != null) {
        LOGGER.error("They have the cookie");
        LOGGER.info(cookie.getValue());
        context.response().write("You found my cookie!").end();
      }else{
        LOGGER.error("There is no cookie");
        context.response().write("You have no cookies...").end();}
    } catch (Exception e){
      LOGGER.error("Something happened");
      //context.response().write("What happened?").end();
      e.printStackTrace();
      context.response().setChunked(true).end();
    }
  }
}
