package occ.hww.recommend;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.jdbc.SQL92JDBCDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.JDBCDataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import com.sap.db.jdbcext.DataSourceSAP;

public class RecommendEngine {

	public static void main(String[] args) throws TasteException {

		DataSourceSAP dataSource = new DataSourceSAP();
		dataSource.setServerName("10.58.114.210");
		dataSource.setPort(30015);
		// dataSource.setUrl("jdbc:sap://10.58.114.210:30015?reconnect=true");
		dataSource.setUser("SYSTEM");
		dataSource.setPassword("manager");
		dataSource.setReconnect("TRUE");
		dataSource.setSchema("I076620_MASTER_OCC");

		JDBCDataModel dataModel = new SQL92JDBCDataModel(dataSource,
				"taste_preferences", "user_id", "item_id", "preference", null);

		DataModel model = dataModel;
		UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(2,
				similarity, model);
		Recommender recommender = new GenericUserBasedRecommender(model,
				neighborhood, similarity);

		List<RecommendedItem> recommendations = recommender.recommend(3, 3);
		System.out.println(recommendations);
		System.out.println(model);
		try {
			PreferenceArray preferenceArray = model.getPreferencesFromUser(2);
			preferenceArray.sortByValue();
			System.out.println(preferenceArray);
		} catch (TasteException e) {
			e.printStackTrace();
		}

		System.out.println("connection successfully!");
	}

	private static void getConnection() {
		String uri = "jdbc:sap://10.58.114.210:30015?reconnect=true";
		String user = "SYSTEM";
		String password = "manager";
		String dbName = "I076620_MASTER_OCC";

		try {
			Class.forName("com.sap.db.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		Connection conn;
		try {
			conn = DriverManager.getConnection(uri, user, password);
			Statement stmt = conn.createStatement();
			stmt.execute("SET SCHEMA " + dbName);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
