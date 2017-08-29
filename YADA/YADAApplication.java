import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
* Description: To run our Database Project
* 				Application
* 
* @author Chandni Pakalapati
* 
*/

public class YADAApplication {

	/**
	 * main method to drive the application
	 * @param args
	 */
	
	public static void main(String args[]){

		System.out.println("==================================="
				+ "=================");
		System.out.println("      WELCOME TO Yelp Academic "
				+ "DataSet Analysis");
		System.out.println("==================================="
				+ "=================");
		DataSetAnalysis da = new DataSetAnalysis();
		String option;

		do{
			System.out.println("Enter Choice: ");
			System.out.println("1. Analysis ");
			System.out.println("2. CRUD Operation ");
			Scanner scan = new Scanner(System.in);
			int choice = scan.nextInt();
			switch(choice){
			case 1 : 
				try {
				da.firstAnalysis();
				} catch (Exception e) {
				e.printStackTrace();
				}
				break;
			
			case 2: 
				try {
					CRUD.Operations();
				} catch (IOException | SQLException e1) {
					e1.printStackTrace();
				}
				break;
			
			default: System.out.println("Incorrect option");
				break;
			}
			
			System.out.println("Would you like"
					+ " to continue?[Y/N]");
			option=scan.next();

		}while( (option.equals("y")) || (option.equals("Y")));
	}
}
