package org.wso2.carbon.social.adaptor;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.ndatasource.common.DataSourceException;
import org.wso2.carbon.ndatasource.core.CarbonDataSource;
import org.wso2.carbon.ndatasource.core.DataSourceManager;


public class DSConnection {
	private static final Log log = LogFactory.getLog(DSConnection.class);

	public static Connection getConnection() throws SQLException,
			DataSourceException {
		Connection conn;
		try {
			PrivilegedCarbonContext.startTenantFlow();
			PrivilegedCarbonContext privilegedCarbonContext = PrivilegedCarbonContext
					.getThreadLocalCarbonContext();
			privilegedCarbonContext.setTenantId(Constants.SUPER_TENANT_ID);
			privilegedCarbonContext
					.setTenantDomain(Constants.SUPER_TENANT_DOMAIN);
			CarbonDataSource carbonDataSource = DataSourceManager.getInstance()
					.getDataSourceRepository()
					.getDataSource(Constants.SOCIAL_DB_NAME);
			DataSource dataSource = (DataSource) carbonDataSource.getDSObject();
			conn = dataSource.getConnection();
			return conn;
		} catch (SQLException e) {
			log.error("Can't create JDBC connection to the SQL Server", e);
			throw e;
		} catch (DataSourceException e) {
			log.error("Can't create data source for SQL Server", e);
			throw e;
		} finally {
			PrivilegedCarbonContext.endTenantFlow();
		}
	}

	public static void closeConnection(Connection connection) {
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			log.warn("Can't close JDBC connection to the SQL server", e);
		}
	}
	
	public static String getDatabaseType(Connection conn) throws Exception {
		String type = null;
		try {
			if (conn != null && (!conn.isClosed())) {
				DatabaseMetaData metaData = conn.getMetaData();
				String databaseProductName = metaData.getDatabaseProductName();
				if (databaseProductName.matches("(?i).*hsql.*")) {
					type = "hsql";
				} else if (databaseProductName.matches("(?i).*derby.*")) {
					type = "derby";
				} else if (databaseProductName.matches("(?i).*mysql.*")) {
					type = "mysql";
				} else if (databaseProductName.matches("(?i).*oracle.*")) {
					type = "oracle";
				} else if (databaseProductName.matches("(?i).*microsoft.*")) {
					type = "mssql";
				} else if (databaseProductName.matches("(?i).*h2.*")) {
					type = "h2";
				} else if (databaseProductName.matches("(?i).*db2.*")) {
					type = "db2";
				} else if (databaseProductName.matches("(?i).*postgresql.*")) {
					type = "postgresql";
				} else if (databaseProductName.matches("(?i).*openedge.*")) {
					type = "openedge";
				} else if (databaseProductName.matches("(?i).*informix.*")) {
					type = "informix";
				} else {
					String msg = "Unsupported database: " + databaseProductName +
							". Database will not be created automatically by the WSO2 ES. " +
							"Please create the database using appropriate database scripts for " +
							"the SOCIAL database.";
					throw new Exception(msg);
				}
			}
		} catch (SQLException e) {
			String msg = "Failed to create Social database." + e.getMessage();
			log.fatal(msg, e);
			throw new Exception(msg, e);
		}
		return type;
	}

}
