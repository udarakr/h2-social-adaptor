package org.wso2.carbon.social.adaptor;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.ndatasource.common.DataSourceException;
import org.wso2.carbon.social.core.Activity;
import org.wso2.carbon.social.core.SocialActivityException;

import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class PaginationAdaptor {
	private static final Log log = LogFactory.getLog(PaginationAdaptor.class);
	private static JsonParser parser = new JsonParser();
	private static String selectSQLDesc = null;
	private static String selectSQLAsc = null;
	private static String selectSQLPopular = null;

	public static List<Activity> paginate(String targetId, String tenant,
			String order, int limit, int offset) throws SocialActivityException {
		String errorMsg = "Unable to generate the resultset";
		PreparedStatement statement;
		ResultSet resultSet;
		List<Activity> activities = null;
		try {
			Connection conn = DSConnection.getConnection();
			String selectQuery = getSelectquery(conn, order);
			statement = conn.prepareStatement(selectQuery);
			statement.setString(1, targetId);
			statement.setString(2, tenant);
			statement.setInt(3, limit);
			statement.setInt(4, offset);

			resultSet = statement.executeQuery();

			activities = new ArrayList<Activity>();
			while (resultSet.next()) {
				JsonObject body = (JsonObject) parser.parse(resultSet
						.getString(Constants.BODY_COLUMN));
				int id = resultSet.getInt(Constants.ID_COLUMN);
				Activity activity = new SQLActivity(body);
				activity.setId(id);
				activities.add(activity);
			}
			resultSet.close();

		} catch (SQLException e) {
			String message = errorMsg + e.getMessage();
			log.error(message, e);
			throw new SocialActivityException(message, e);
		} catch (DataSourceException e) {
			String message = errorMsg + e.getMessage();
			log.error(message, e);
			throw new SocialActivityException(message, e);
		}
		return activities;

	}

	private static String getSelectquery(Connection connection, String order)
			throws SocialActivityException {
		String SQL;
		String type = "select";
		if ("NEWEST".equals(order)) {
			if (selectSQLDesc == null) {
				// TODO remove info log
				log.info("selectSQLDesc not found. setting up.. ");
				selectSQLDesc = getSelectSQL(connection, order, type);
			}
			SQL = selectSQLDesc;
		} else if ("OLDEST".equals(order)) {
			if (selectSQLAsc == null) {
				// TODO remove info log
				log.info("selectSQLAsc not found. setting up.. ");
				selectSQLAsc = getSelectSQL(connection, order, type);
			}
			SQL = selectSQLAsc;
		} else {
			if (selectSQLPopular == null) {
				// TODO remove info log
				log.info("selectSQLPopular not found. setting up.. ");
				selectSQLPopular = getSelectSQL(connection, order,
						type);
			}
			SQL = selectSQLPopular;
		}
		return SQL;
	}
	
	public static String getSelectSQL(Connection connection, String key, String queryType)
			 throws SocialActivityException {

					try {

						JsonObject jsonObject = readJson(connection);
						JsonObject selectSQLObject = (JsonObject) jsonObject.get(queryType);
						String sql = selectSQLObject.get(key).getAsString();

						if (sql != null) {
							return sql;
						} else {
							throw new SocialActivityException(
									"Unable to locate the query related to, type: "
											+ queryType + " key: " + key);
						}

					} catch (FileNotFoundException e) {
						log.error(e.getMessage());
						throw new SocialActivityException(e.getMessage(), e);
					} catch (JsonIOException e) {
						log.error(e.getMessage());
						throw new SocialActivityException(e.getMessage(), e);
					} catch (JsonSyntaxException e) {
						log.error(e.getMessage());
						throw new SocialActivityException(e.getMessage(), e);
					} catch (Exception e) {
						log.error(e.getMessage());
						throw new SocialActivityException(e.getMessage(), e);
					}
				}
				
				private static JsonObject readJson(Connection connection)
						throws SocialActivityException, JsonIOException,
						JsonSyntaxException, FileNotFoundException {
					String databaseType;
					try {
						databaseType = DSConnection.getDatabaseType(connection);
					} catch (Exception e) {
						log.error(e.getMessage());
						throw new SocialActivityException(e.getMessage(), e);
					}
					if (log.isDebugEnabled()) {
						log.debug("Loading select query for " + databaseType);
					}

					String carbonHome = System.getProperty("carbon.home");
					String dbJsonLocation = carbonHome
							+ "/dbscripts/social/sql-scripts.json";
					Object obj = parser.parse(new FileReader(dbJsonLocation));
					JsonObject jsonObject = (JsonObject) obj;
					JsonObject dbTypeObject = (JsonObject) jsonObject.get(databaseType);
					
					return dbTypeObject;

				}


}
