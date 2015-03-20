package org.wso2.carbon.social.adaptor;

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
import org.wso2.carbon.social.sql.Constants;
import org.wso2.carbon.social.sql.DSConnection;
import org.wso2.carbon.social.sql.SQLActivity;
import org.wso2.carbon.social.sql.SocialUtil;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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
				selectSQLDesc = SocialUtil
						.getSelectSQL(connection, order, type);
			}
			SQL = selectSQLDesc;
		} else if ("OLDEST".equals(order)) {
			if (selectSQLAsc == null) {
				// TODO remove info log
				log.info("selectSQLAsc not found. setting up.. ");
				selectSQLAsc = SocialUtil.getSelectSQL(connection, order, type);
			}
			SQL = selectSQLAsc;
		} else {
			if (selectSQLPopular == null) {
				// TODO remove info log
				log.info("selectSQLPopular not found. setting up.. ");
				selectSQLPopular = SocialUtil.getSelectSQL(connection, order,
						type);
			}
			SQL = selectSQLPopular;
		}
		return SQL;
	}

}
