import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.Buffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class CRUDapplication {

	public static void main(String[] args) throws MongoException, Exception {
		// TODO Auto-generated method stub

		/**** Get database ****/
		// if database doesn't exists, MongoDB will create it for you
		DB db = (new MongoClient("localhost", 27017).getDB("Innovative"));
		BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(System.in));

		databseOperation(db, bufferedReader);
	}

	private static void databseOperation(DB db, BufferedReader bufferedReader)
			throws IOException, ParseException {
		// TODO Auto-generated method stub

		// if collection doesn't exists, MongoDB will create it for you
		DBCollection test = db.getCollection("CRUD");

		String userInput = null;
		while (true) {
			System.out.println("----------------------------------------------------------------------------------------------");
			System.out.println("Please give specific input(String/Indexes) for your desired Operations :- ");
			System.out.println("----------------------------------------------------------------------------------------------");
			
			System.out.println("Which operation do you want to do ?");
			
			System.out.println("<1>Insert");
			System.out.println("<2>Search");
			System.out.println("<3>Update");
			System.out.println("<4>Delete");
			System.out.println("<5>SpecificSearch");
			System.out.println("<6>AdvanceSearch");
			System.out.println("<7>Distinct Search");
			System.out.println("<8>Search By Sorting");
			System.out.println("<9>Text Advanced Search");
			System.out.println("<10>Specific Column Data");
			System.out.println("<11>Map Reduce");
			System.out.println("<12>Number of Documents");
			System.out.println("<13>Regular Expression");

			System.out.println("<14>Exit");

			userInput = bufferedReader.readLine();
			if (userInput.equals("Exit") || userInput.equals("exit") || userInput.equals("14"))
				break;
			else if (userInput.equals("Search") || userInput.equals("search") || userInput.equals("2"))
				findAll(test);
			else if (userInput.equals("Insert") || userInput.equals("insert") || userInput.equals("1"))
				insertData(bufferedReader, test);
			else if (userInput.equals("Delete") || userInput.equals("delete") || userInput.equals("4"))
				deleteData(bufferedReader, test);
			else if (userInput.equals("Update") || userInput.equals("update") || userInput.equals("3"))
				updateData(bufferedReader, test);
			else if (userInput.equals("SpecificSearch")
					|| userInput.equals("specificsearch") || userInput.equals("5"))
				spcificsearchData(bufferedReader, test);
			else if (userInput.equals("AdvanceSearch")
					|| userInput.equals("advancesearch") || userInput.equals("6"))
				advancedsearchData(bufferedReader, test);
			else if (userInput.equals("MapReduce")
					|| userInput.equals("mapreduce") || userInput.equals("11"))
				mapReduceData(bufferedReader, test);

			else if (userInput.equals("Number of Documents")
					|| userInput.equals("number of documents") || userInput.equals("12"))
				countDocuments(bufferedReader, test);
			else if (userInput.equals("Distinct Search")
					|| userInput.equals("distinct search") || userInput.equals("7"))
				distinctData(bufferedReader, test);

			else if (userInput.equals("Search by sorting")
					|| userInput.equals("search by sorting") || userInput.equals("8"))
				sortingData(bufferedReader, test);

			else if (userInput.equals("specific column data")
					|| userInput.equals("Specific Column Data") || userInput.equals("10"))
				specificData(bufferedReader, test);
			
			else if (userInput.equals("Text Advanced Search")
					|| userInput.equals("text advanced search")
					|| userInput.equals("9"))
				textAdvanced(bufferedReader, test);
			else if (userInput.equals("Regular Expresion")
					|| userInput.equals("regular expression") || userInput.equals("13"))
				regExpData(bufferedReader, test);
			
		}

	}

	private static void regExpData(BufferedReader bufferedReader,
			DBCollection test) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("it gives the document whose initial of First_Name is  : ");
		String userI=bufferedReader.readLine();
		
		DBObject obj1= new BasicDBObject();
		obj1.put("$regex", "^"+userI);
		
		DBObject obj2= new BasicDBObject();
		obj2.put("First_Name", obj1);
		
		DBCursor cursor=test.find(obj2);
		while(cursor.hasNext()){
			System.out.println(cursor.next());
		}
		
		
	}

	private static void textAdvanced(BufferedReader bufferedReader,
			DBCollection test) throws IOException,MongoException {
		// TODO Auto-generated method stub
		
		DBObject obj1=new BasicDBObject();
		obj1.put("Description","text");
		
		System.out.println(obj1.toString()+"-----EnsureIndex Value");
		
		test.ensureIndex(obj1);
		System.out.println("Please write any middle word of Description Key : :  ");
		String user=bufferedReader.readLine();
		
		DBObject obj=new BasicDBObject();
		obj.put("$search",user);
		
		DBObject obj2=new BasicDBObject();
		obj2.put("$text",obj);
				
		DBCursor cursor=test.find(obj2);
		while(cursor.hasNext()){
			System.out.println(cursor.next());
		}
		
		
		
		
	}

	private static void specificData(BufferedReader bufferedReader,
			DBCollection test) throws IOException {
		// TODO Auto-generated method stub
		
		System.out.println("Specific columns data that you want to show");
		String input = bufferedReader.readLine();
		
		String[] specificInput = input.split(" ");

		DBObject obj = new BasicDBObject();
		
		for (int i = 0; i < specificInput.length; i++) {
			obj.put(specificInput[i], 1);
		}
		
		System.out.println(obj.toString());
		
		
		DBCursor cursor = test.find(null,obj);
		System.out.println(cursor.count());
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}

	}

	private static void sortingData(BufferedReader bufferedReader,
			DBCollection test) throws IOException {
		// TODO Auto-generated method stub

		while (true) {
			System.out
					.println("wants to sort data by ASCENDING order or DESCENDING order : : : ");
			System.out.println("<1>Ascending Order");
			System.out.println("<2>Descending Order");
			String userInput = bufferedReader.readLine();
			if (userInput.equals("1") || userInput.equals("Ascending Order")
					|| userInput.equals("ascending order")) {
				DBObject object = new BasicDBObject();
				object.put("Age", 1);
				DBCursor cursor = test.find().sort(object);
				while (cursor.hasNext()) {
					System.out.println(cursor.next());
				}
				break;
			} else if (userInput.equals("2")
					|| userInput.equals("Descending Order")
					|| userInput.equals("descending order")) {
				DBObject object = new BasicDBObject();
				object.put("Age", -1);
				DBCursor cursor = test.find().sort(object);
				while (cursor.hasNext()) {
					System.out.println(cursor.next());
				}
				break;
			} else {
				System.out.println("PLEASE give proper Input! ");

			}
		}
	}

	private static void distinctData(BufferedReader bufferedReader,
			DBCollection test) throws IOException {
		// TODO Auto-generated method stub
		
		System.out.println("Please, Give the value of perticular Key: : :");
		String input = bufferedReader.readLine();
		List distinct = test.distinct(input);

		System.out.println("Your output is as below");
		int i = 0;
		while (i < distinct.size()) {
			System.out.println(distinct.get(i));
			i++;
		}

	}

	private static void countDocuments(BufferedReader bufferedReader,
			DBCollection test) {
		// TODO Auto-generated method stub
		Long count = test.count();
		System.out.println("Total Number of Records/Documents are : : : "
				+ count);
	}

	private static void mapReduceData(BufferedReader bufferedReader,
			DBCollection test) {
		// TODO Auto-generated method stub

		
		// map function categorize YOUNG PEOPLE.
		String ageMap = "function(){" + "var criteria;" + "if(this.Age>22) {"
				+ "criteria='YoungPeople'; " + "emit(criteria,this.Balance);" + "}"
				+ "};";
		// reduce function to add all the BALANCE values of persons whose age > 22....
		String balReduce = "function(key,Balance){return Array.sum(Balance);"
				+ "};";
		System.out.println(ageMap+"-------------");
		System.out.println(balReduce+"-------------");
		
		// create the mapreduce command by calling map and reduce functions
		MapReduceCommand mapcmd = new MapReduceCommand(test, ageMap, balReduce,
				null, MapReduceCommand.OutputType.INLINE, null);

		// invoke the mapreduce command
		MapReduceOutput ages = test.mapReduce(mapcmd);

		// print the sum of Balance of all the entities whose age > 22
		for (DBObject o : ages.results()) {

			System.out.println(o.toString());

		}
		
		
	}

	private static void advancedsearchData(BufferedReader bufferedReader,
			DBCollection test) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("Specific search by First Name: ");
		String firstIn = bufferedReader.readLine();

		System.out.println("............OR.............");

		System.out.println("Specific search by Last Name: ");
		String lastIn = bufferedReader.readLine();

		/**
		 * DBCursor cursor= test.find( 
		 * 	{$or:[ 
		 * 			{"First Name",firstIn},
		 * 			{"First Name",lastIn}, 
		 * 		 ]
		 *  });
		 **/

		DBObject fromDBObject = new BasicDBObject();
		fromDBObject.put("First_Name", firstIn);

		DBObject toDBObject = new BasicDBObject();
		toDBObject.put("Last_Name", lastIn);

		
		BasicDBList or = new BasicDBList();
		or.add(fromDBObject);
		or.add(toDBObject);

		DBObject query = new BasicDBObject("$or", or);
		DBCursor cursor = test.find(query);
		while (cursor.hasNext())
			System.out.println(cursor.next());

	}

	private static void spcificsearchData(BufferedReader bufferedReader,
			DBCollection test) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("Specific search by First Name: ");
		String fromDatabase = bufferedReader.readLine();

		DBObject fromDBObject = new BasicDBObject();
		fromDBObject.put("First_Name", fromDatabase);

		DBCursor Cursor = test.find(fromDBObject);
		while (Cursor.hasNext())
			System.out.println(Cursor.next());

	}

	private static void updateData(BufferedReader bufferedReader,
			DBCollection test) throws IOException {
		// TODO Auto-generated method stub

		// user can read the content
		findAll(test);

		// take input as First Name that you want to update
		System.out.println("Update from First Name : ");
		String fromDatabase = bufferedReader.readLine();

		// create a document(fromDBObject) to store key and value
		DBObject fromDBObject = new BasicDBObject();
		fromDBObject.put("First Name", fromDatabase);

		// Give name that you want to UPDATE
		System.out.println("Update to First Name : ");
		String toDatabase = bufferedReader.readLine();

		// create a document(toDBObject) to store key and value
		DBObject toDBObject = new BasicDBObject();
		toDBObject.put("First Name", toDatabase);

		// create a document(updateDBObject) to store key and value
		DBObject updateDBObject = new BasicDBObject();
		updateDBObject.put("$set", toDBObject);

		// KEY=DBObject of Stored Data in your Collection,
		// VALUE=DBObject that you want to change data
		test.update(fromDBObject, updateDBObject);

		System.out.println("\n\n");
		// user can check the content
		findAll(test);
		System.out.println("\n\n");

	}

	private static void deleteData(BufferedReader bufferedReader,
			DBCollection test) throws IOException {
		// TODO Auto-generated method stub

		// user can read the content
		findAll(test);

		System.out
				.println("Which one(record/Document) do you want to delete ??? ");
		String delRecord = bufferedReader.readLine();

		// create a document to store key and value

		BasicDBObject delRaw = new BasicDBObject();
		delRaw.put("First_Name", delRecord);
		test.remove(delRaw);

		// user can read the output
		findAll(test);

	}

	private static void insertData(BufferedReader bufferedReader,
			DBCollection test) throws IOException, ParseException {
		// TODO Auto-generated method stub
		System.out.println("Your First Name is : : ");
		String firstName = bufferedReader.readLine();
		System.out.println("Your Last Name : : : ");
		String lastName = bufferedReader.readLine();

		System.out.println("Description : ");
		String descr = bufferedReader.readLine();
		
		System.out.println("Pet Name: ");
		String petName = bufferedReader.readLine();
		
		System.out.println("Your age : : : ");
		int age = Integer.parseInt(bufferedReader.readLine());

		System.out.println("Your Date-of-Birth : : : ");

		String birth = bufferedReader.readLine();
		SimpleDateFormat x = new SimpleDateFormat("yyyy-MM-dd");
		Date d1 = x.parse(birth);
		
		

		
		System.out.println("Your Balance : : : ");
		String balance = bufferedReader.readLine();
		Long bal = Long.parseLong(balance);

		System.out.println("Give Account names: : : ");
		String accountDetail = bufferedReader.readLine();
		
		BasicDBList list=new BasicDBList();
		
		String[] account = accountDetail.split(" ");
		list.add(account);
		
		/**** Insert ****/
		// create a document to store key and value

		BasicDBObject document = new BasicDBObject();

		document.put("First_Name", firstName);
		document.put("Last_Name", lastName);
		document.put("Description", descr);
		document.put("Pet_Name", petName);
		
		document.put("Age", age);
		document.put("BirthDate", d1);
		document.put("Today's_Date", new Date());
		document.put("Balance", bal);
		document.put("Account", list);
		
		test.insert(document);
		// test.insert(arr)

		findAll(test);
	}

	private static void findAll(DBCollection test) {
		// TODO Auto-generated method stub

		DBCursor Cursor = test.find();
		while (Cursor.hasNext())
			System.out.println(Cursor.next());
	}

}
