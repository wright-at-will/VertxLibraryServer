package BookAuditWebServer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.vertx.core.http.Cookie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.asyncsql.MySQLClient;

public class MainVerticle extends AbstractVerticle {

	String url = "jdbc:mysql://easel2.fulgentcorp.com/yby805" + "?" + "db=yby805"
			+ "&useLegacyDatetimeCode=false&useTimezone=true&serverTimezone=GMT ";

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
		LOGGER.info("Stopping verticle");
		client.close();
		promises.complete();
	}

	private Future<Void> connectDB() {
		Promise<Void> promise = Promise.promise();
		JsonObject connectOptions = new JsonObject().put("host", "easel2.fulgentcorp.com").put("username", "yby805")
				.put("password", "y9X8yYS2ZsFsuK1Xlzgj").put("database", "yby805");

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
		router.route("/").handler(this::index);
		router.route("/login").handler(this::login);
		router.route("/reports/bookdetail").handler(this::getBookReport);

		server.requestHandler(router).listen(8888, http -> {
			if (http.succeeded()) {
				promise.complete();
				System.out.println("HTTP server started on port 8888");
			} else {
				promise.fail(http.cause());
			}
		});
		return promise.future();
	}

  //INFO Route Controller for "/"
  private void index(RoutingContext context) {
    context.request().response().putHeader("Content-Type", "text/html");
    context.request().response().end(
      "<H1>Welcome to Assignment 4</H1>\nUse extensions /login?username=bob&password=1234 or /reports/bookdetail");
  }

  //INFO Route controller for "/login"
	private void login(RoutingContext context) {
		HttpServerResponse loginResponse = context.response();
		connect(context, loginResponse);
    /*client.getConnection(ar -> {
      if(ar.failed()) {
        LOGGER.error("Could not open a db connection", ar.cause());
        loginResponse.end();
        return;
      }
      SQLConnection connection = ar.result();
      JsonArray userParams = new JsonArray();
      connection.query("SELECT username, HEX(password) from user", feedback -> {
        List<JsonArray> row = feedback.result().getResults();
        if(row.size()<1) {
          LOGGER.error("Command failed");
          connection.close();
          return401StatusCode(context);
        }
        for(JsonArray jsa: row){
          LOGGER.info(jsa.toString());
        }
      });
    });*/
		//loginResponse.end();
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
			connection.queryWithParams("SELECT * FROM user WHERE username=? and password=UNHEX(?)", userParams, feedback -> {
				List<JsonArray> row = feedback.result().getResults();
				if (row.size() < 1) {
					LOGGER.error("Could not find " + username + " in our DB");
					connection.close();
					return401StatusCode(context);
					return;
				}

				JsonArray createSessionParams = new JsonArray();
				createSessionParams.add(row.get(0).getInteger(0));
				createSessionParams.add(row.get(0).getString(1));
				connection.updateWithParams(
						"INSERT INTO session (user_id, token, expiration) "
								+ "VALUES (?, UNHEX(SHA2 ( CONCAT( NOW(), ? ), 256)), DATE_ADD( NOW(), INTERVAL 1 MINUTE))",
						createSessionParams, resultHandler -> {
							if (resultHandler.failed()) {
								LOGGER.error("Failed to create a session with a valid username and password: "
										+ username + ". ", resultHandler.cause());
								loginResponse.end("Error creating session");
								return;
							}
							LOGGER.info("token received");

							//JsonArray getTokenParams = resultHandler.result().getKeys();
							connection.querySingle(" select HEX(token) from session where id=(SELECT LAST_INSERT_ID());",
									tokenHandler -> {
							  if(tokenHandler.failed())
							    LOGGER.error("Did not get token");
							  loginResponse
                  .addCookie(Cookie.cookie("SessionCookie",tokenHandler.result().getString(0)).setMaxAge(1000))
                  .end("Your sessionCookie should have: "+tokenHandler.result().getString(0));
							  return;
							});
						});
				connection.close();
				//loginResponse.end();
			});
		});
	}

	private void generateJSON(RoutingContext context, HttpServerResponse loginResponse, String username,
			SQLConnection connection, AsyncResult<JsonArray> tokenHandler) {
		LOGGER.info("Entering generateToken: Closed connection");
		connection.close();
		if (tokenHandler.failed()) {
			LOGGER.error("Could not retrieve token ", tokenHandler.cause());
			return401StatusCode(context);
		}

		JsonArray sessionID = tokenHandler.result();
		JsonObject JSON = new JsonObject();
		if (sessionID.size() > 0) {
			JSON.put("response", "ok");
			JSON.put("session token", sessionID);
			LOGGER.info("Session successfully created for " + username);
			loginResponse.putHeader("Content-Type", "text/json");
			loginResponse.end(JSON.encodePrettily());
		} else {
			loginResponse.putHeader("Content-Type", "text/html");
			loginResponse.end("Something went wrong when attempting to show token JSON");
		}
	}
//INFO Route controller for "/reports/bookdetail
	private void getBookReport(RoutingContext context) {
		HttpServerResponse serverResponse = context.response();
		LOGGER.info("Entered getBookReport ");
		String bearerToken = null;
		try {
			// "Bearer "
			bearerToken = context.request().getHeader("Authorization").substring(7);
		} catch (Exception e) {
			return401StatusCode(context);
		}
		JsonArray token = new JsonArray().add(bearerToken);

		client.getConnection(ar -> {
			if (ar.failed()) {
				LOGGER.error("Error opening DB", ar.cause());
				serverResponse.end("Error opening DB");
				return;
			}

			SQLConnection connection = ar.result();
			/*
			 * "SELECT allowed_fields FROM session, permissions WHERE session.token=? " +
			 * "AND allowed_fields=1 " + "AND session.expiration > NOW() " +
			 * "AND session.user_id=permissions.user_id"
			 */
			connection.querySingleWithParams("SELECT * FROM session WHERE expiration > now() and token=UNHEX(?)", token,
					resultHandler -> {
						if (resultHandler.failed()) {
							LOGGER.error("Select query failed", resultHandler.cause());
							connection.close();
							return401StatusCode(context);
							return;
						} else {
							if (resultHandler.result() == null) {
								LOGGER.error("Not a valid token found. ");
								connection.close();
								return401StatusCode(context);
								return;
							}

							XSSFWorkbook workbook = new XSSFWorkbook();
							XSSFSheet sheet = workbook.createSheet("Book Report");
							addReportTitle(sheet, 0);
							connection.query(
									"SELECT title, publisher_name, year_published " + "FROM Book, publisher "
											+ "WHERE Book.publisher_id=publisher.id "
											+ "ORDER BY publisher.publisher_name, Book.title",
									excelQuery -> createExcelSheet(workbook, sheet, serverResponse, excelQuery));
						}
					});
		});
	}

	private void createExcelSheet(XSSFWorkbook workbook, XSSFSheet sheet, HttpServerResponse serverResponse,
			AsyncResult<ResultSet> excelQuery) {
		addBookRecord(sheet, excelQuery.result().getRows(), 2);
		String fileName = "book_report.xlsx";
		saveReportAs(workbook, fileName);
		serverResponse.putHeader("Content-Type", "application/vnd.ms-excel");
		serverResponse.putHeader("Content-Disposition", "attachment;filename=" + fileName);
		serverResponse.sendFile(fileName).end();
	}

	private void addReportTitle(XSSFSheet sheet, int startRow) {
		XSSFRow row = sheet.createRow(startRow);
		Cell cell = row.createCell(0);
		cell.setCellValue("Publisher and Book");
	}

	private void addBookRecord(XSSFSheet sheet, List<JsonObject> records, int startRow) {
		addBookHeaders(sheet, startRow);
		addBooks(sheet, records, startRow + 1);
		addSummary(sheet, records, startRow + records.size() + 2);
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

	private void addSummary(XSSFSheet sheet, List<JsonObject> records, int startRow) {
		XSSFRow row = sheet.createRow(startRow);
		row.createCell(0).setCellValue("Total Publishers");
		row.createCell(2).setCellValue(getNumPublishers(records));

		row = sheet.createRow(startRow + 1);
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


	private void return401StatusCode(RoutingContext context) {
		context.response().setStatusCode(401);
		context.response().end();
	}

}
