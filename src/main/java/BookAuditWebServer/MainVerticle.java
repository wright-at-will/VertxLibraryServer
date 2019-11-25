package BookAuditWebServer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.asyncsql.MySQLClient;

public class MainVerticle extends AbstractVerticle {

  String url = "jdbc:mysql://easel2.fulgentcorp.com/yby805" +
    "?" +
    "db=yby805" +
    "&useLegacyDatetimeCode=false&useTimezone=true&serverTimezone=GMT ";
  
  private SQLClient client;
  private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

  @Override
  public void start(Promise<Void> promise) throws Exception {
    Future<Void> steps = connectDB().compose(v -> startHttpServer());
	LOGGER.info("Starting verticle");
    steps.setHandler(ar -> {
    	if (ar.succeeded()) {
    		promise.complete();
    	} else {
    		promise.fail(ar.cause());
    	}
    });
  }
  
  @Override
  public void stop(Promise<Void> promises) throws Exception {
	  super.stop(promises);
	  LOGGER.info("Stopping verticle");
	  client.close();
	  promises.complete();
  }

  private Future<Void> connectDB(){
    Promise<Void> promise = Promise.promise();
    JsonObject connectOptions = new JsonObject()
      .put("host", "easel2.fulgentcorp.com")
      .put("user", "yby805")
      .put("password", "y9X8yYS2ZsFsuK1Xlzgj")
      .put("database", "yby805");
    
    client = MySQLClient.createShared(vertx, connectOptions);
    
    
    if (client != null) 
      LOGGER.info("Client opened");
    else 
      LOGGER.info("Client Failed to open");
    
    promise.complete();
    return promise.future();
  }

  private Future<Void> startHttpServer() {
    Promise<Void> promise = Promise.promise();
    HttpServer server = vertx.createHttpServer();
    Router router = Router.router(vertx);
    router.route("/").handler(this::login);
    router.route("/login").handler(this::login);
    router.route("/reports/bookdetail").handler(this::getBookReport);

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
	  HttpServerResponse loginResponse = context.response();
	  connect(context, loginResponse);
	  
    //LOGGER.info("I am sending a cookie back");
    //loginResponse.addCookie(Cookie.cookie("MyCookie","10").setMaxAge(10));
    loginResponse.end();
    //TODO get DB connection and make a cookie with sessionToken = SHA2( CONCAT( NOW(), ‘my secret value’ ) , 256)
    //connect(client, Promise.promise(), "SHA2( CONCAT( NOW(), ?),256");
  }
  
  private void index(RoutingContext context) {
	  context.request().response().putHeader("content-type", "text/plain");
	  context.request().response().end();
  }

  private void connect(RoutingContext context, HttpServerResponse loginResponse) {
	  String username = context.request().getParam("username");
	  String hashedPassword = context.request().getParam("password");
	  
    client.getConnection(ar -> {
      if (ar.failed()) {
        LOGGER.error("Could not open a database connection", ar.cause());
        loginResponse.end("Failure to open database");
        return;
      } 
        SQLConnection connection = ar.result();
        JsonArray userParams = new JsonArray();
        userParams.add(username).add(hashedPassword);
        String userQuery = "SELECT * FROM user WHERE username=? and password=?";
        connection.queryWithParams(userQuery, userParams, feedback -> {
        	if (feedback.result().getNumRows() < 1) {
        		loginResponse.setStatusCode(401).end();
        		return;
        	}
        	
        	int userId = feedback.result().getRows().get(0).getInteger("id");
        	JsonArray createSessionParams = new JsonArray().add(userId);
        	String createSessionQuery = "INSERT INTO session (user_id, token, expiration) VALUES (?, SHA2(UUID(), DATE_ADD( NOW(), INTERVAL 1 MINUTE))";
        	connection.updateWithParams(createSessionQuery, createSessionParams, resultHandler -> {
        		if (resultHandler.failed()) {
        			LOGGER.error("Failed to create a session with a valid user");
        			loginResponse.end("Error creating session");
        			return;
        		}
        		int newID = resultHandler.result().getKeys().getInteger(0);
        		JsonArray getTokenParams = new JsonArray().add(newID);
        		String getTokenQuery = "SELECT token FROM session WHERE id=?";
        		connection.queryWithParams(getTokenQuery, getTokenParams, tokenHandler -> {
        			String token = tokenHandler.result().getRows().get(0).getString("token");
        			loginResponse.putHeader("Content-Type", "text/json");
        			JsonObject JSON = new JsonObject();
        			JSON.put("response", "ok");
        			JSON.put("session token", token);
        			//close loginResponse
        			loginResponse.end(JSON.toString());
        					
        		});
        	});
        });
    });
  }

  private void getBookReport(RoutingContext context){
	  String token = context.request().getHeader("Authorization").replace("Bearer ", "");
	  HttpServerResponse serverResponse = context.response();
	  
	  client.getConnection(ar -> {
		  if (ar.failed()) {
			  LOGGER.error("Error opening DB", ar.cause());
			  serverResponse.end("Error opening DB");
			  return;
		  }
		  
		  SQLConnection connection = ar.result();
		  JsonArray verifyParamPermissions = new JsonArray();
		  verifyParamPermissions.add(token);
		  String verifyQueryPermissions = "SELECT allowed FROM session, permissions WHERE session.token=? "
				  + "AND allowed=1 "
				  + "AND session.expiration > NOW() "
				  + "AND session.user_id=permissions.user_id";
		  connection.queryWithParams(verifyQueryPermissions, verifyParamPermissions, resultHandler -> {
			  if (resultHandler.result().getNumRows() < 1) {
				  serverResponse.setStatusCode(401).end();
				  return;
			  }
			  XSSFWorkbook workbook = new XSSFWorkbook();
			  XSSFSheet sheet = workbook.createSheet("Book Report");
			  addReportTitle(sheet, 0);
			  String query = "SELECT title, publisher_name, year_published "
					  + "FROM book, publisher "
					  + "WHERE book.publisher_id=publisher.id "
					  + "ORDER BY publisher.publisher_name, book.title";
			  connection.query(query, excelQuery -> {
				  addBookRecord(sheet, excelQuery.result().getRows(), 2);
				  String fileName = "book_report.xlsx";
				  saveReportAs(workbook, fileName);
				  serverResponse.putHeader("Content-Type", "application/vnd.ms-excel");
				  serverResponse.putHeader("Content-Disposition", "attachment;filename=" + fileName);
				  serverResponse.sendFile(fileName).end();				  
			  });
		  });
	  });
  }
  
  private void addReportTitle(XSSFSheet sheet, int startRow) {
	  XSSFRow row = sheet.createRow(startRow);
	  Cell cell = row.createCell(0);
	  cell.setCellValue("Publisher and Book");
  }
  
  private void addBookRecord(XSSFSheet sheet, List<JsonObject> records, int startRow) {
	  addBookHeaders(sheet, startRow);
	  addBooks(sheet, records, startRow+1);
	  addSummary(sheet, records, startRow+records.size()+2);
	  sheet.autoSizeColumn(0);
	  sheet.autoSizeColumn(1);
	  sheet.autoSizeColumn(2);
  }
  
  private void addBookHeaders(XSSFSheet sheet, int startRow) {
	  XSSFRow row = sheet.createRow(startRow);
	  row.createCell(0).setCellValue("Book Title");
	  row.createCell(0).setCellValue("Publisher");
	  row.createCell(0).setCellValue("Year Published");
  }
  
  private void addBooks(XSSFSheet sheet, List<JsonObject> records, int startRow) {
	  int currentRow = startRow;
	  for (JsonObject record : records) {
		  String title = record.getString("title");
		  String publisherName = record.getString("publisher_name");
		  int yearPublished = record.getInteger("year_published");
		  
		  
		  XSSFRow nextRow = sheet.createRow(currentRow);
		  nextRow.createCell(0).setCellValue(title);
		  nextRow.createCell(0).setCellValue(publisherName);
		  nextRow.createCell(0).setCellValue(yearPublished);
		  currentRow++;	  
	  }
  }
  
  private void addSummary(XSSFSheet sheet, List <JsonObject> records, int startRow) {
	  XSSFRow row = sheet.createRow(startRow);
	  row.createCell(0).setCellValue("Total Publishers");
	  row.createCell(2).setCellValue(getNumPublishers(records));
	  
	  row = sheet.createRow(startRow+1);
	  row.createCell(0).setCellValue("Total Books");
	  row.createCell(2).setCellValue(records.size());
  }
  
  private int getNumPublishers(List<JsonObject> records) {
	  Set<String> publishers = new HashSet<>();
	  for (JsonObject record : records) 
		  publishers.add(record.getString("publisher_name"));
	  return publishers.size();
  }
  
  private void saveReportAs(XSSFWorkbook workbook, String fileName) {
		try {
			FileOutputStream out = new FileOutputStream(new File(fileName));
			workbook.write(out);
			out.close();
		} catch (IOException e) {
				e.printStackTrace();
		} 
		
  }
  
}