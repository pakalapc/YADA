import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;

/**
* Description: i. To perform basic CRUD operation
* 				on our database and also to insert 
* 			   ii. To populate the new tables
* 				with the dataset.
* 
* @author Chandni Pakalapati
* 
*/

public class CRUD {

	private static Connection con;
	private static final String url = 
			"jdbc:postgresql://reddwarf.cs.rit.edu/ps7723";
	private static boolean debug = false;

	private static PreparedStatement citystmt;
	private static PreparedStatement neighstmt;
	private static PreparedStatement categorystmt;
	private static PreparedStatement businessstmt;
	private static PreparedStatement userstmt;
	private static PreparedStatement checkinstmt;
	private static PreparedStatement reviewstmt;
	private static PreparedStatement businessstmt_1;
	private static PreparedStatement userstmt_1;
	private static PreparedStatement category;
	private static PreparedStatement city;

	/**
	 * default constructor
	 */
	public CRUD(){
		
	}
	/** parameterized constructor
	 * 
	 * @param dbUser	String
	 * @param dbPassword	String
	 */
	
	public CRUD(String dbUser, String dbPassword) {
		try {
			Class.forName("org.postgresql.Driver");
			con = DriverManager.getConnection(url, dbUser, dbPassword);
		
			citystmt = con.prepareStatement
					("INSERT INTO yada_city VALUES (?,?,?)");
			neighstmt = con.prepareStatement
					("INSERT INTO yada_neighborhood VALUES (?,?,?)");
			
			categorystmt = con.prepareStatement
					("INSERT INTO yada_category VALUES (?,?)");
			businessstmt = con.prepareStatement
					("INSERT INTO yada_business VALUES (?,?,?,?,?,?,?,?,?,?,?)");
			
			userstmt = con.prepareStatement
					("INSERT INTO yada_user3 VALUES (?,?,?,?,?,?)");

			checkinstmt = con.prepareStatement
					("INSERT INTO yada_checkinginfo VALUES (?,?,?)");
			
			reviewstmt = con.prepareStatement
					("INSERT INTO yada_review VALUES (?,?,?,?)");
			
			businessstmt_1 = con.prepareStatement
					("SELECT business_id from yada_business ");
			
			userstmt_1 = con.prepareStatement
					("select user_id from yada_user3");
			
			category=con.prepareStatement
					("select category from yada_category where category "
					+ "like ?");
			
			city=con.prepareStatement
					("select name,state from yada_city where name "
					+ "like ?");
			
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			System.out.println("Cannot connect to the database");
			return;
		}
	}	
	
	/**
	 * splitString to split the string
	 * @param line	String
	 * @return	String Array
	 */
	private static String[] splitString(String line) {

		String[] values = line.split(",");
		String[] value_tmp = new String[values.length];
		for (int i = 0; i < values.length; i++) {
			value_tmp[i] = values[i].replaceAll("\"", "").replaceAll("'", "\'")
			.trim();
		}

		return value_tmp;
	}

	/**
	 * getNeighborKey to retrieve the 
	 * neighbourhood_id
	 * 
	 * @param neighbor	String
	 * @param city	String	
	 * @param state	String
	 * @return key	int
	 */
	
	private static int getNeighborKey
	(String neighbor, String city, String state) {

		String keyString = neighbor + "," + city + "," + state;
		int key = keyString.hashCode();
		return key;
	}
	
	/**
	 * getCityKey to retrieve the city_id
	 * 
	 * @param city	String
	 * @param state	String
	 * @return	key	int
	 */
	
	private static int getCityKey(String city, String state) {

		String keyString = city + "," + state;
		int key = keyString.hashCode();
		return key;
	}
	
	/**
	 * getCategoryKey to get the category_id
	 * 
	 * @param categoryName	String
	 * @return key	int
	 */
	
	private static int getCategoryKey(String categoryName) {

		return categoryName.trim().hashCode();
	}
	
	/**
	 * getPrimaryKeyBusiness to get the 
	 * business_id
	 * 
	 * @throws SQLException
	 * @throws FileNotFoundException
	 */
	
	public static void getPrimaryKeyBusiness() 
			throws SQLException, FileNotFoundException{
		
		PrintWriter pr = new PrintWriter(new File("businesspkey.csv"));
		ArrayList<String> business_id=new ArrayList<String>();
		ResultSet rs=businessstmt_1.executeQuery();
		while (rs.next()) {
			String business=rs.getString("business_id");
			pr.print(business+',');
		}
		//System.out.println(business_id.toString());
	    pr.close();	
	}
	
	/**
	 * getPrimaryKeyUser to get the user_id
	 * 
	 * @throws SQLException
	 * @throws FileNotFoundException
	 */
	
	public static void getPrimaryKeyUser() 
			throws SQLException, FileNotFoundException{
		
		PrintWriter pr = new PrintWriter(new File(" userpkey.csv"));
		ArrayList<String> user_id=new ArrayList<String>();
		ResultSet rs=userstmt_1.executeQuery();
		while (rs.next()) {
			String business=rs.getString("user_id");	
			pr.print(business+',');
		}
		//System.out.println(business_id.toString());
	    pr.close();	
	}
	
	/**
	 * insertBusinessFromFile to read business 
	 * data from file
	 * 
	 * @param filename	String
	 * @return	boolean
	 * @throws IOException
	 * @throws SQLException
	 */
	public boolean insertBusinessFromFile(String filename) throws IOException,
		SQLException {
		BufferedReader fread = null;
		try {
			fread = new BufferedReader(new FileReader(new File(filename)));
			String line = null;
			while ((line = fread.readLine()) != null) {

					String[] values = splitString(line);
					insertBusiness(values[0], values[1], values[2], values[6],
							values[7], values[8], values[4], values[5], values[11],
							values[10], values[9]);
			}
			return true;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("File not found : " + filename + " exception:"
			+ e);

		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error reading file exception " + e);
		} finally {
			fread.close();
		}
		return false;
	}
	
	/**
	 * insertCityFromFile to read city details
	 * from the file
	 * 
	 * @param filename	String
	 * @return	boolean
	 * @throws IOException
	 * @throws SQLException
	 */
	
	public boolean insertCityFromFile(String filename) throws IOException,
	SQLException {
		BufferedReader fread = null;
		try {
			fread = new BufferedReader
					(new FileReader(new File(filename)));
			String line = null;
			int count =0;
			while ((line = fread.readLine()) != null) {

				String[] values = splitString(line);
				String hash=String.valueOf(values[1].hashCode());
				if(insertCity(hash,values[1], values[0])) {
					count ++;
				}
				if( count >500){
					System.out.println("committed");
					con.commit();count =0;
			}
		}
		return true;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println
			("File not found : " + filename + " exception:"
			+ e);

		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error reading file exception " + e);
		} finally {
			fread.close();
		}
		return false;
	}
	
	/**
	 * insertCheckinFromFile to read Checking
	 * details from the file
	 * 
	 * @param filename	String
	 * @return	boolean
	 * @throws IOException
	 * @throws SQLException
	 */

	public boolean insertCheckinFromFile(String filename)
			throws IOException,
		SQLException {
		BufferedReader fread = null;
		try {
			fread = new BufferedReader
					(new FileReader(new File(filename)));
			String line = null;
			while ((line = fread.readLine()) != null) {
				String[] values = splitString(line);
				insertCheckinInfo(values[0], values[1], values[2]);
			}
			return true;
		
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("File not found : " + filename + " exception:"
					+ e);
		
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error reading file exception " + e);
		} finally {
			fread.close();
		}
		return false;
	}

	/**
	 * insertCategoryFromFile to read Category
	 * details from file
	 * 
	 * @param filename String
	 * @return	boolean
	 * @throws IOException
	 * @throws SQLException
	 */

	public boolean insertCategoryFromFile(String filename)
			throws IOException,SQLException {
		BufferedReader fread = null;
		try {
			fread = new BufferedReader(new FileReader(new File(filename)));
			String line = null;
			while ((line = fread.readLine()) != null) {
		
				String[] values = splitString(line);
				insertCategory(values[0]);
			}
			return true;
		
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("File not found : " + filename + " exception:"
					+ e);
		
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error reading file exception " + e);
		} finally {
			fread.close();
		}
		return false;		
	}

	/**
	 * insertReviewFromFile to read review details 
	 * from file
	 * 
	 * @param filename	String
	 * @return	boolean
	 * @throws IOException
	 * @throws SQLException
	 */

	public boolean insertReviewFromFile(String filename) 
			throws IOException,SQLException {
		BufferedReader fread = null;
		try {
		fread = new BufferedReader
				(new FileReader(new File(filename)));
		String line = null;
		while ((line = fread.readLine()) != null) {
		
			String[] values = splitString(line);
			insertReview(values[0], values[1], values[2], values[3]);
		}
		return true;
	
		} catch (FileNotFoundException e) {	
			e.printStackTrace();
			System.out.println("File not found : " + filename + " exception:"
		+ e);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error reading file exception " + e);
		} finally {
			fread.close();
		}
		return false;
	}

	/**
	 * insertNeighborhoodFromFile to read neighborhood
	 * details from File
	 * @param filename
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 */
	
	public boolean insertNeighborhoodFromFile(String filename)
			throws IOException, SQLException {
		BufferedReader fread = null;
		try {
			fread = new BufferedReader(new FileReader(new File(filename)));
			String line = null;
			while ((line = fread.readLine()) != null) {

				String[] values = splitString(line);
				insertNeighborhood(values[0], values[1], values[2]);
			}
			return true;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("File not found : " + filename + " exception:"
			+ e);

		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error reading file exception " + e);
		} finally {
			fread.close();
		}
		return false;
	}
	
	/**
	 * insertCategory to insert category data
	 * in the DB
	 * @param categoryName
	 * @return
	 * @throws SQLException
	 */
	
	private static boolean insertCategory(String categoryName)
			throws SQLException {

		int categorykey = getCategoryKey(categoryName);
		categorystmt.setInt(1, categorykey);
		categorystmt.setString(2, categoryName);
		if (debug) {
			System.out.println(" Query:category table : " + categorykey + " "
					+ categoryName);
		}
		try {
			categorystmt.executeUpdate();
			return true;
		} catch (SQLException e) {

			if (!e.getMessage().contains("duplicate")) {
				e.printStackTrace();
				System.out.println(" Cannot insert query category table:  "
						+ categorykey + " " + categoryName);
			}
		}
		return false;
	}

	/**
	 * insertNeighborhood to insert neighborhood
	 * data into DB
	 * 
	 * @param neighborhoodName	String
	 * @param city	String
	 * @param state	String
	 * @return	boolean
	 * @throws SQLException
	 */
	
	private static boolean insertNeighborhood(String neighborhoodName,
			String city, String state) throws SQLException {

		int citykey = getCityKey(city, state);
		int neighkey = getNeighborKey(neighborhoodName, city, state);
		neighstmt.setInt(1, neighkey);
		neighstmt.setString(2, neighborhoodName);
		neighstmt.setInt(3, citykey);
		if (debug) {
			System.out.println(" Query:neighbor table : " + neighkey + " "
					+ neighborhoodName + " " + citykey);
		}
		try {
			neighstmt.executeUpdate();
			return true;
		} catch (SQLException e) {

			if (!e.getMessage().contains("duplicate")) {
				e.printStackTrace();
				System.out.println(" Cannot insert query:  " + neighkey + " "
						+ neighborhoodName + " " + citykey);
			}
		}
		return false;
	}

	/**
	 * insertCheckinInfo to insert checking
	 * details in DB
	 * 
	 * @param businessId	String
	 * @param noOfCheckin	String
	 * @param dayTime	String
	 * @return	boolean
	 * @throws SQLException
	 */
	
	private static boolean insertCheckinInfo(String businessId,
		String noOfCheckin, String dayTime) throws SQLException {

		checkinstmt.setInt(2, Integer.parseInt(noOfCheckin));
		checkinstmt.setString(1, businessId);
		checkinstmt.setInt(3, Integer.parseInt(dayTime));
	
		if (debug) {
			System.out.println(" checkinInfo table : " + businessId + " "
					+ noOfCheckin + " " + dayTime);
		}
		try {
			checkinstmt.executeUpdate();
			return true;
		} catch (SQLException e) {
			if (!e.getMessage().contains("duplicate")) {
				e.printStackTrace();
				System.out.println(" Cannot insert query checkinInfo table:  "
						+ businessId + " " + noOfCheckin + " " + dayTime);
			}
		}
		return false;
	}
	
	/**
	 * insertReview to insert review details 
	 * in the DB
	 * 
	 * @param reviewId	String
	 * @param userId	String
	 * @param businessId	String
	 * @param star	String
	 * @return	boolean
	 * @throws SQLException
	 */
	
	private static boolean insertReview(String reviewId,
		String userId, String businessId, String star ) throws SQLException {

		reviewstmt.setInt(4, Integer.parseInt(star));
		reviewstmt.setString(3, businessId);
		reviewstmt.setString(2, userId);
		reviewstmt.setString(1, reviewId);
		if (debug) {
			System.out.println(" review table : " + reviewId + " "
					+ userId + " " + businessId+" "+star);
		}
		try {
			reviewstmt.executeUpdate();
			return true;
		} catch (SQLException e) {
	
			if (!e.getMessage().contains("duplicate")) {
				e.printStackTrace();
				System.out.println(" review table : " + reviewId + " "
						+ userId + " " + businessId+" "+star);
			}
		}
		return false;
	}

	/**
	 * insertBusiness to insert business details in 
	 * the DB
	 * @param BusinessId	String
	 * @param is_open	String
	 * @param address	String
	 * @param longitude	String
	 * @param latitude	String
	 * @param star_recieved	String
	 * @param city	String
	 * @param state	String
	 * @param categoryName	String
	 * @param neighborhoodName	String
	 * @param businessName	String
	 * @return	boolean
	 * @throws SQLException
	 */
	
	private static boolean insertBusiness(String BusinessId, String is_open,
			String address, String longitude, String latitude,
			String star_recieved, String city, String state,
			String categoryName, String neighborhoodName,String businessName)
			throws SQLException {

		int categorykey = getCategoryKey(categoryName);
		int neighborhoodkey = getNeighborKey(neighborhoodName, city, state);
		int citykey = getCityKey(city, state);
		//Math.round(((longitude_tmp*5)-90)*10000.0)/10000.0;
		Double longi = (double) Math.round(((Double.parseDouble(longitude)+90)/5)*10000.0/10000.0);
		Double lati = (double) Math.round(((Double.parseDouble(latitude)+180)/10)*10000.0/10000.0);
		String geomap= "(" + longi + "," + lati +")";
		try {
			businessstmt.setString(1, BusinessId);
			businessstmt.setString(2, is_open);
			businessstmt.setString(3, address);
			businessstmt.setDouble(4, Double.parseDouble(longitude));
			businessstmt.setDouble(5, Double.parseDouble(latitude));
			businessstmt.setDouble(6, Double.parseDouble(star_recieved));
			businessstmt.setInt(7, citykey);
			businessstmt.setInt(8, categorykey);
			businessstmt.setInt(9, neighborhoodkey);
			businessstmt.setString(10, businessName);
			businessstmt.setString(11, geomap);
		} catch (NumberFormatException e) {
			System.out.println("Query:business table : " + BusinessId + " "
					+ is_open + " " + address + " " + longitude + " "
					+ latitude + " " + star_recieved + " " + citykey + " "
					+ categorykey + " " + neighborhoodkey + " " + businessName);
		}catch (SQLException e) {
			System.out.println
			("Incorrect data type for one of the attributes : "+ BusinessId + " "
					+ is_open + " " + address + " " + longitude + " "
					+ latitude + " " + star_recieved + " " + citykey + " "
					+ categorykey + " " + neighborhoodkey + " " + businessName);
			System.out.println
			("Usage: business_id(String)  is_open(String)  address(String) "
					+ " longitude(Double)  latitude(Double) star recieved(Double)"
					+ "city(String) category(String)"
					+ "  Neighborood(String)  business Name (String)");
		}
		if (debug) {
			System.out.println(" Query:business table : " + BusinessId + " "
					+ is_open + " " + address + " " + longitude + " "
					+ latitude + " " + star_recieved + " " + citykey + " "
					+ categorykey + " " + neighborhoodkey + " " + businessName);
		}
		try {
			if(businessstmt.executeUpdate()==0){
				System.out.println("Failed in Inserting");
			}
			else{
				System.out.println("Record Inserted");
			}
			return true;
		} catch (SQLException e) {

			if (!e.getMessage().contains("duplicate")) {
				e.printStackTrace();
				System.out.println(" Cannot insert query business table:  "
						+ BusinessId + " " + is_open + " " + address + " "
						+ longitude + " " + latitude + " " + star_recieved
						+ " " + citykey + " " + categorykey + " "
						+ neighborhoodkey + " " + businessName);
			}
		}
		return false;
	}

	/**
	 * inputForBusiness taking the business
	 * details from the user
	 * 
	 * @return	boolean
	 * @throws IOException
	 */
	
	private static boolean inputForBusiness() throws IOException{
		BufferedReader br =new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Please provide following"
				+ " information comma seperated: ");
		System.out.println("Usage: Business Name (String)  is_open(Yes/No)"
				+ "  address(String) "
				+ " longitude(Double)  latitude(Double) star recieved(Double)"
				+ " city(String) state(String) category(String)  Neighborood(String)  ");
		
		String[] values = br.readLine().split(",");
		String business_id = String.valueOf(values[0].hashCode());
		try {
			insertBusiness(business_id, values[1], values[2], 
					values[3], values[4], values[5], values[6], 
					values[7], values[8], values[9],values[0]);
		} catch (SQLException e) {
			System.out.println(" Cannot insert "
					+ "business data.Error: "+ e.getMessage());
		}
		return true;
	}
	
	/**
	 * readBusiness to retrieve business 
	 * data from the DB
	 */
	
	private static void readBusiness()  {
		BufferedReader br =new BufferedReader
				(new InputStreamReader(System.in));
		System.out.print("Enter the name of the business: ");
		String business_name;
		try {
			business_name = br.readLine();
			//String hash=String.valueOf(business_name.hashCode());
			//System.out.println("Hascode: " + hash);
			String query=
					("Select * from yada_business where name='"+ business_name+"'");
			
			System.out.println("Query: " + query);
			Statement stmt;
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			String name = null,is_open=null,add=null,
					city=null,cat_id=null,category=null,
					state=null;
			Double longitude = null,latitude=null,star=null;
			
			while(rs.next()){
				name=rs.getString(10);
				is_open=rs.getString(2);
				add=rs.getString(3);
				longitude=rs.getDouble(4);
				latitude=rs.getDouble(5);
				star=rs.getDouble(6);
				city=rs.getString(7);
				cat_id=rs.getString(8);
			}
			query=("Select category from yada_category"
					+ " where category_id='" +cat_id+"'");
			rs=stmt.executeQuery(query);
			while(rs.next()){
				category=rs.getString(1);
			}
			query=("Select name,state from yada_city"
					+ " where city_id='" +city+"'");
			rs=stmt.executeQuery(query);
			while(rs.next()){
				city=rs.getString(1);
				state=rs.getString(2);
			}
			System.out.println("Business Name-> " + name + "\n" +
								"Category->" + category + "\n" +
								"is_open-> " + is_open + "\n" +
								"Address-> " + add+ "\n" +
								"Longitude->" + longitude + "\n" +
								"Latitude->" + latitude + "\n" +
								"Stars_received->" + star + "\n" +
								"City-> " + city +"\n" +
								"State-> " + state);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			System.out.println("No such business is prsent.");
		}	
	}

	/**
	 * updateBusiness to take the 
	 * details from user
	 * to update the business
	 * 
	 * @return	boolean
	 * @throws NumberFormatException
	 * @throws IOException
	 * @throws SQLException
	 */
	
	private static boolean updateBusiness() 
			throws NumberFormatException, IOException, SQLException{
		BufferedReader br =new BufferedReader(new InputStreamReader(System.in));
		String address = null,business = null,city = null,state = null,
				latitude=null,longitude=null,neighborhood=null,
				is_open=null;
			System.out.println("Enter your business-name:");
			business = br.readLine();
			String hash=String.valueOf(business.hashCode());
			System.out.println("Insert your option to update:");
			System.out.println("Address: ");
			address = br.readLine();
			System.out.println("City: ");
			city = br.readLine();
			System.out.println("Longitude: ");
			longitude = br.readLine();
			System.out.println("Latitude: ");
			latitude = br.readLine();
			System.out.println("State:");
			state=br.readLine();
			System.out.println("Neigborhood: ");
			neighborhood = br.readLine();
			System.out.println("Enter your business status:[Yes/No]");
			is_open=br.readLine();
		
		String full_address =address+","+city+","+state;
		updateBusiness(business, is_open, full_address, longitude, 
				latitude, city, state, neighborhood);
		return true;
	}
	

	/**
	 * updateBusiness to update the business
	 * details in the DB
	 * 
	 * @param BusinessId	String
	 * @param is_open	String
	 * @param address	String
	 * @param longitude	String
	 * @param latitude	String
	 * @param city	String
	 * @param state	String
	 * @param neighborhoodName	String
	 * @return
	 * @throws SQLException
	 */
	
	private static boolean updateBusiness(String business, String is_open,
			String address, String longitude, String latitude,
			String city, String state,
			String neighborhoodName)
			throws SQLException {

		//int categorykey = getCategoryKey(categoryName);
		int neighborhoodkey = getNeighborKey(neighborhoodName, city, state);
		int citykey = getCityKey(city, state);
		String query=("Update yada_business set is_open='"+is_open+"',"
				+ "address='"+address+"',"
						+ "longitude="+longitude+","
								+ "latitude="+latitude+","
						+ "city_id="+citykey+","
								+ "neighborhood_id="+ neighborhoodkey+
						"where name='"+business+"'");
		try {
			Statement stmt;
			stmt = con.createStatement();
			stmt.execute(query);
			return true;
		} catch (SQLException e) {
			System.out.println("Failed updating!");
		}
		return false;
	}
		
	/**
	 * deleteBusiness to delete a business
	 * detail from the DB
	 * 
	 * @return boolean
	 * @throws IOException
	 */
	
	public static boolean deleteBusiness() throws IOException{
		try {
			BufferedReader br=new BufferedReader
					(new InputStreamReader(System.in));
			System.out.print("Enter Business name: ");
			String business_name = br.readLine();
			PreparedStatement delete=
					con.prepareStatement
					("Delete from yada_business where business_id=?");
			delete.setString(1, String.valueOf((business_name.hashCode()))) ;
			if(delete.executeUpdate() == 0){
				System.out.println("No such record to delete.");
			}
			else{
				System.out.println("Record deleted!");
			}
			return true;
		} catch (SQLException e) {
			System.out.println("Error in generating delete sql"
					+ " query fo business "+e.getMessage());
			return false;
		}
	}

	/**
	 * insertUser to insert user details
	 * in the DB
	 * 
	 * @param avgStars	String
	 * @param name	String
	 * @param noOfReviews	String
	 * @param usefulVotes	String
	 * @return
	 * @throws SQLException
	 */
	
	private static boolean insertUser(String avgStars,
			String name, String noOfReviews, String usefulVotes)
			throws SQLException {

		int id = 0;
		try {
			String query = "SELECT max(unique_id) from yada_user3" ;
			Statement stmt;
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				id=rs.getInt(1);
			}
			userstmt.setString(1, ""+(id+1));
			userstmt.setDouble(2, Double.parseDouble(avgStars));
			userstmt.setString(3, name);
			userstmt.setInt(4, Integer.parseInt(noOfReviews));
			userstmt.setInt(5, Integer.parseInt(usefulVotes));
			userstmt.setInt(6,id+1);
			System.out.println("ID generated: " +(id+1));
		} catch (NumberFormatException e) {
			return true;
		}

		try {
			userstmt.executeUpdate();
			System.out.println("Record Inserted");
			return true;
		} catch (SQLException e) {

			if (!e.getMessage().contains("duplicate")) {
				e.printStackTrace();
				System.out.println(" Cannot insert query user table:  "
						+ id+1 + " " + avgStars + " " + name + " "
						+ noOfReviews + " " + usefulVotes);	
			}
		}
		return false;

	}
	/**
	 * inputForUser to take the 
	 * user related details from the user
	 * 
	 * @return	boolean
	 * @throws IOException
	 * @throws SQLException
	 */
	
	private static boolean inputForUser()throws IOException, SQLException{
		BufferedReader br =new BufferedReader
				(new InputStreamReader(System.in));
		System.out.println("Please provide following "
				+ "information comma seperated: ");
		System.out.println("Usage: User Name (String)"
				+ "  Stars(Integer)  no.of Reviews(Integer) "
				+ " useful votes(Integer)");
		String[] values = br.readLine().split(",");
		insertUser(values[1], values[0], values[2], values[3]);
		return true;
	}
	
	/**
	 * readUser to retrieve
	 * user details from the DB
	 */
	
	private static void readUser(){
		BufferedReader br =new BufferedReader
				(new InputStreamReader(System.in));
		System.out.print("Enter the user's unique id: ");
		int unique_id;
		try {
			unique_id = Integer.parseInt(br.readLine());
			Statement stmt;
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * "
					+ "FROM yada_user3 where unique_id=" + unique_id);
			while (rs.next()){
				String userid= rs.getString(1);
				int avgStars= rs.getInt(2);
				String name = rs.getString(3);
				int noofreviews = rs.getInt(4);
				int usefulvotes = rs.getInt(4);
				System.out.println("ID-> " + userid + "\n" + 
									"Avg_Stars-> " + avgStars + "\n" +
									"Name-> " + name + "\n" + 
									"No Of Reviews-> " + noofreviews +"\n" +
									"Useful Votes->" + usefulvotes);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			System.out.println("No such record is present.");
		}

	}
	
	/**
	 * deleteUser to delete user details from
	 * DB
	 */
	
	private static void deleteUser(){
		BufferedReader br =new BufferedReader
				(new InputStreamReader(System.in));
		System.out.print("Enter the user's unique id to be deleted: ");
		int unique_id;
		try {
			unique_id = Integer.parseInt(br.readLine());
			PreparedStatement delete=con.prepareStatement
					("Delete  from yada_user3 where unique_id="+unique_id+"");
			if(delete.executeUpdate()==0){
				System.out.println("Failed in deleting!");
			}
			else{
				System.out.println("DELETED record for: " + unique_id);
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			System.out.println("No such record is present.");
		}
	}
	
	/**
	 * insertCity to insert city details in
	 * the DB
	 * @param id	String
	 * @param city	String
	 * @param state	String
	 * @return	boolean
	 * @throws SQLException
	 */
	
	private static boolean insertCity(String id,
			String city, String state)
			throws SQLException {

		try {
			citystmt.setInt(1, Integer.parseInt(id));
			citystmt.setString(2, city);
			citystmt.setString(3, state);
		} catch (NumberFormatException e) {
			return true;
		}
		try {
			if(citystmt.executeUpdate()==0){
				System.out.println("Falied in Inserting.");
			}
			else{
				System.out.println("Record inserted");
			}
			return true;
		} catch (SQLException e) {

			if (!e.getMessage().contains("duplicate")) {
				//e.printStackTrace();
				System.out.println(" Cannot insert record!");
			}
		}
		return false;
	}

	/**
	 * inputForCity to take the
	 * city details from the user
	 * 
	 */
	
	private static boolean inputForCity() throws IOException{
		BufferedReader br =new BufferedReader
				(new InputStreamReader(System.in));
		System.out.println("Please provide following "
				+ "information comma seperated: ");
		System.out.println("Usage: city Name (String) State(String)");
		
		String[] values = br.readLine().split(",");
		String city_id = String.valueOf(values[0].hashCode());
		try{
				insertCity(city_id, values[0], values[1]);
				} catch (SQLException e) {
				System.out.println(" Cannot insert business"
						+ " data.Error: "+ e.getMessage());
			}
		return true;
	}
	
	/**
	 * readCity to retrieve city 
	 * details from the DB
	 */
	
	private static void readCity(){
		BufferedReader br =new BufferedReader
				(new InputStreamReader(System.in));
		try {
			System.out.println("Enter a letter to list"
					+ " the cities starting with it ");
			String cityletter=br.readLine().toUpperCase();
			city.setString(1,cityletter + "%");
			ResultSet rs = city.executeQuery();
			System.out.println("City" + "\t" +"\t" 
			+"\t" + "State");
			System.out.println("----------------------------------");
			while (rs.next()){
				String city_name= rs.getString(1);
				String state = rs.getString(2);
				System.out.println(city_name + "\t" + "\t" +
								state);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			System.out.println("No record is present.");
		}
	}

	/**
	 * readCategory to retrieve Category
	 * details from DB
	 */
	
	private static void readCategory(){
		try{
			System.out.println("Enter a letter to list"
					+ " the categories starting with it ");
			Scanner scan = new Scanner(System.in);
			String option=scan.next().toUpperCase();
			category.setString(1,option + "%");
			ResultSet rs =category.executeQuery();
			while (rs.next()){
				String category= rs.getString(1);
				System.out.println(category); 
			}
		} catch (Exception e) {
		System.out.println("No such record is present.");
		}
	}
	
	/**
	 * unAuthorized to provide
	 * restrictions on certain DB
	 * operations
	 */
	
	private static void unAuthorized(){
		System.out.println("You do not have the"
				+ " access to perform this operation!");
	}
	
	/**
	 * performCRUD to perform the
	 * required CRUD operation on the table
	 * 
	 * @param database	String
	 * @return	boolean
	 * @throws NumberFormatException
	 * @throws IOException
	 * @throws SQLException
	 */
	
	private static boolean performCRUD(String database)
			throws NumberFormatException,
	IOException, SQLException {

		System.out.println(" \n\n-----> Welcome to " + database+ 
				" Table Console <-------");
		boolean correct=false;
		int choice=0;
		while(!correct){
			System.out.println("Select the below Operation:\n"
			+ "1. Create[1]\n"
			+ "2. Read[2]\n"
			+ "3. Update[3]\n"
			+ "4. Delete[4]\n"
			+ "5. Exit[5]");
			System.out.println("Option:");
			
			BufferedReader br =new BufferedReader(new 
					InputStreamReader(System.in));
			choice = Integer.parseInt(br.readLine());
			if(choice == 5){
					return true;
			}
			if ( choice > 0 && choice <6 ){
				correct = true;
			}else {
				System.out.println("Incorrect option selected");	
			}
		}
		
		switch (choice) {
		case 1:
			if(database.equals("yada_business")){
				inputForBusiness();
			}
			if (database.equals("user")){
				inputForUser();
			}
			if (database.equals("category")){
				unAuthorized();
			}
			if (database.equals("city")){
				inputForCity();
			}
		
			break;
		
		case 2:
			if(database.equals("yada_business")){
				readBusiness();
			}
			if (database.equals("user")){
				readUser();
			}
			if (database.equals("category")){
				readCategory();
			}
			if (database.equals("city")){
				readCity();
			}
			break;
		
		case 3:
			if(database.equals("yada_business")){
				updateBusiness();
			}
			if (database.equals("user")){
				unAuthorized();
			}
			if (database.equals("category")){
				unAuthorized();
			}
			if (database.equals("city")){
				unAuthorized();
			}
			break;
		
		case 4:
			if(database.equals("yada_business")){
				deleteBusiness();
			}
			if (database.equals("user")){
				deleteUser();
			}
			if (database.equals("category")){
				unAuthorized();
			}
			if (database.equals("city")){
				unAuthorized();
			}
			break;
		
		default:
		break;
		}
		
		return true;
		
		}
	
	/**
	 * userCLI to provide
	 * the options to the user
	 * 
	 * @return	boolean
	 * @throws NumberFormatException
	 * @throws IOException
	 * @throws SQLException
	 */
	
	public static boolean userCLI()
			throws NumberFormatException, IOException, SQLException {
		System.out.println("=====================================");
		System.out.println("    Welcome to YADA Database CLI");
		System.out.println("=====================================");
		boolean correct=false;
		int choice=0;
		while(!correct){
			System.out.println("Select the Table to perform CRUD operation\n"
				+ "1. Business[1]\n"
				+ "2. User[2]\n"
				+ "3. City[3]\n"
				+ "4. Category[4]\n"
				+ "5. Exit[5]");
			System.out.print("Option:");
			BufferedReader br =new BufferedReader
					(new InputStreamReader(System.in));
			choice = Integer.parseInt(br.readLine());
			if(choice == 5){
				return true;
			}
			if ( choice > 0 && choice <6 ){
			correct = true;
			}else {
			System.out.println("Incorrect option selected");	
			}
		}
		switch (choice) {
		case 1:
			performCRUD("yada_business");	
			break;
			
		case 2:
			performCRUD("user");
			break;

		case 3:
			performCRUD("city");
			break;

		case 4:
			performCRUD("category");
			break;
			
		default:
			break;
		}

		return true;
	}
	
	/**
	 * Operations to perform 
	 * CRUD operations on DB
	 * 
	 * @throws IOException
	 * @throws SQLException
	 */
	
	public static void Operations()
			throws IOException, SQLException {

		
		Scanner scan = new Scanner(System.in);
		String option;
		String dbUser = "";
		String dbPassword = "";
		try {
			dbUser = "ps7723";
			dbPassword = "Priy57";
		}
		catch (Exception e) {
			System.err.println("Error reading password.");
			e.printStackTrace();
		}
		CRUD PSQL = new CRUD(dbUser, dbPassword);
		
		do{
			userCLI();
			System.out.println("Do you want to"
					+ " continue CRUD operations?[Y/N]");
			option=scan.next();
		}while( (option.equals("y")) || (option.equals("Y")));
		System.out.println("Thanks for using YADA DB Console!");
	}
}