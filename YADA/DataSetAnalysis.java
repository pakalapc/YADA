import java.sql.SQLException;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Set;
import weka.clusterers.SimpleKMeans;
import weka.core.Instance;
import weka.core.Instances;
import weka.experiment.InstanceQuery;
import java.util.*;

/**
 * Description: To perform analysis on the 
 * 				YELP dataset and help
 * 				user to take decisions
 * 
 * @author Chandni Pakalapati
 *  
 */

public class DataSetAnalysis {

	static Set<String> set = new HashSet<String>();
	static Set<String> set1 = new HashSet<String>();
	InstanceQuery query;
	String category;
	Instances data;
	SimpleKMeans kmeans;

	/**
	 * menu to display all the possible
	 * categories of possible we would 
	 * be able to help in
	 * 
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws Exception
	 */

	public void menu()throws ClassNotFoundException,
	SQLException, Exception {

		ArrayList<String> menu = new ArrayList<String>();
		query= new InstanceQuery();
		query.setUsername("ps7723");
		query.setPassword("Priy57");
		query.setQuery("SELECT category FROM yada_category order by category");
		data = query.retrieveInstances();
		Instance inst;

		for(int i = 0; i < data.numInstances();i++){
			inst = data.instance(i);
			menu.add(inst.toString());
		}

		for(String i : menu){
			System.out.print(i+"\t");
		}
		System.out.println();
	}

	/**
	 * firstAnalysis to let the user 
	 * know if his business at his desired
	 * location is appropriate for him
	 * 
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws Exception
	 */

	public void firstAnalysis()throws ClassNotFoundException,
	SQLException, Exception {

		menu();
		Scanner scan = new Scanner(System.in);

		//input of the category and the location from the user
		System.out.println("Enter category as displayed in the menu");
		category = scan.next();

		String hashCode = String.valueOf(category.hashCode());
		//		System.out.println("hashcode " + hashCode);
		double[] input=new double[2];
		System.out.println("Enter longitude and latitude");
		input[0] = Double.parseDouble(scan.next());
		input[1] = Double.parseDouble(scan.next());

		//Convert the results of a database query into instances. 

		query= new InstanceQuery();
		query.setUsername("ps7723");
		query.setPassword("Priy57");
		try{
		query.setQuery("SELECT geomap FROM yada_business where category_id="+hashCode);

		data = query.retrieveInstances();

		//Clustering using SimpleKMeans
		kmeans = new SimpleKMeans();
		kmeans.setSeed(10);
		kmeans.setNumClusters(25);
		kmeans.buildClusterer(data); 

		int[] assignments = kmeans.getClusterSizes();
		int i = 0;
		int max_instance=0, max_instance_no=0;
		int min_instance=10000000,min_instance_no=0;

		/*
		 * to get the cluster no with maximum 
		 * and minimum number of instances.
		 */

		for (int clusterNum : assignments) {
			if(clusterNum > max_instance){
				max_instance=clusterNum;
				max_instance_no=i;
			}
			if(clusterNum<min_instance){
				min_instance=clusterNum;
				min_instance_no=i;
			}
			//System.out.printf("Instance %d -> 
			//Cluster %d \n", clusterNum, i);
			i++;
		}

		Instances centroid= kmeans.getClusterCentroids();
		//prints the centroids
		//System.out.println(centroid);

		//dataset it used(consists of unique geomaps) 

		//storing the centroids in the form of long and lat  
		double[][] geomap_centroid= new double[centroid.numInstances()][2];

		/*
		 * converting the centroid to longitude and latitude
		 */
		for (int j = 0; j < centroid.numInstances(); j++) {
			Instance instance = centroid.instance(j);

			String str_instance=instance.toString();
			String geomap_str =
				str_instance.substring(2, str_instance.length()-2);
			double longitude_tmp = 
				Double.parseDouble(geomap_str.split(",")[0]);
			double latitude_tmp = 
				Double.parseDouble(geomap_str.split(",")[1]);

			double longitude =
				Math.round(((longitude_tmp*5)-90)*10000.0)/10000.0;
			double latitude = 
				Math.round(((latitude_tmp*10)-180)*10000.0)/10000.0;
			geomap_centroid[j][0] = longitude;
			geomap_centroid[j][1] = latitude;
		}

		/*
		 * storing the maximum and minimum instance locations.
		 */

		double[][] max_instance_location= new double[1][2];
		double[][] min_instance_location = new double[1][2];
		max_instance_location[0]=geomap_centroid[max_instance_no];
		min_instance_location[0]=geomap_centroid[min_instance_no];
		double average = 
			(assignments[max_instance_no]+assignments[min_instance_no])/2;

		double distance =100000.0;
		double[][] closest_location=new double[1][2];
		int instance_no=0;

		/*
		 * to get the closest known location to the input
		 */
		//System.out.println("Printing instances");
		for (int j = 0; j < geomap_centroid.length; j++) {
			//System.out.println(geomap_centroid[j][0]+ " "+ geomap_centroid[j][1] + " "+ j);
			double temp=
				Math.pow((input[0]-geomap_centroid[j][0]),2) 
				+ Math.pow(input[1]-geomap_centroid[j][1],2);

			if(temp<distance){
				distance = temp;
				closest_location[0]=geomap_centroid[j];
				instance_no=j;
			}	
		}
		System.out.println("User Location: " + input[0] + " " 
				+ input[1] + " " + assignments[instance_no] );
		System.out.println("Max instance location: " + max_instance_location[0][0]
		        + max_instance_location[0][1] + max_instance);
		if(assignments[instance_no] >= average){	
			System.out.println("First Analysis: Yes");
		}else{
			System.out.println("First Analysis: No");
		}
		
/*
		if(assignments[instance_no] >= average){
			System.out.println
			("Its a good place to open business");
			System.out.println
			("No of Operating Business :"+assignments[instance_no]);
			if(assignments[instance_no] < max_instance){
				System.out.println
				("Best location for you would be: "+ max_instance_location[0][0]+" "+ 
						max_instance_location[0][1]+ "\n"
						+"no of operating business here: "+  max_instance);

			}
		}
		else{
			System.out.println
			("This location is less suitable for your business");
			System.out.println
			("There are other Locations where "
					+ "the business category is doing good");
			System.out.println("Best location we provide you : " + max_instance_location[0][0]+" "+ 
					max_instance_location[0][1] );
			System.out.println(max_instance + " business are currently running here.");

		}
*/
		System.out.println("Do you want to proceed with second analysis:[y/n] ");
		String ch = scan.next();
		if((ch.equals("y"))|(ch.equals("Y"))){
			secondAnalysis(instance_no);
		}
		}
		catch(Exception e){
			System.out.println("Sorry! We are unable to help you"
					+ " at this moment.");
		}

	}

	/**
	 * secondAnalysis to let the user 
	 * know about the possible business
	 * he can think of to open
	 * at his location
	 * 
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws Exception
	 */

	public void secondAnalysis(int instance_no)
	throws ClassNotFoundException,SQLException, Exception {
		Instance instance;
		/*
		 * to retrieve the geo maps present in the cluster
		 */
		for (int j=0;j<data.numInstances();j++){
			instance = data.instance(j);
			int no = kmeans.clusterInstance(instance);
			if (no==instance_no){
				set.add(instance.toString());
			}
		}

		/*
		 * to get the where clause containing all desired geomaps
		 */
		StringBuilder whereClause= new StringBuilder();
		for (String a: set){
			whereClause.append(a+",");
		}
		String where_Clause = 
			whereClause.toString().substring(0,whereClause.length()-1);

		/*
		 * to get the dataset from database
		 */

		query= new InstanceQuery();
		query.setUsername("ps7723");
		query.setPassword("Priy57");
		query.setQuery("select category from yada_business b,"
				+ "yada_category c where b.category_id=c.category_id "
				+ "and b.star_received >=3 and b.geomap in"
				+ " (" + where_Clause + ")");
		data = query.retrieveInstances();

		/*
		 * build the second cluster
		 */

		SimpleKMeans kmeans1 = new SimpleKMeans();
		kmeans1.setSeed(10);
		kmeans1.setNumClusters(5);
		kmeans1.buildClusterer(data); 

		int[] assignments = kmeans1.getClusterSizes();

		/*
		 * to get the cluster with maximum instances
		 */
		int s=0,max_instance =0,max_instance_no=0;
		for (int clusterNum : assignments) {
			if(clusterNum > max_instance){
				max_instance=clusterNum;
				max_instance_no=s;
			}
			//System.out.printf("Instance %d -> Cluster %d \n", clusterNum, s);
			s++;
		}

		/*
		 * to get the instances present in the cluster
		 */
		for (int j=0;j<data.numInstances();j++){
			instance = data.instance(j);
			int no = kmeans1.clusterInstance(instance);
			if (no==max_instance_no){
				set1.add(instance.toString());
			}
		}

		if(set1.contains(category)){
			System.out.println("Second Analysis: Yes");
		}
		else{
			System.out.println("Second Analysis: No");
		}
		
		System.out.println("List: ");
		//System.out.println("SET");
		for (String a: set1){
			System.out.println(a);
		}
		
/*
		if(set1.contains(category)){
			System.out.println("You can open your business in your location");
			System.out.println("because we find it to do well.");
		}
		else{
			System.out.println("You may consider any business"
					+ " category from the above list or");
			System.out.println("You may open your business with a risk");
		}
*/
	}
}
