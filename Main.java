import java.sql.*;
import java.util.Scanner;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.io.File;
import java.io.FileNotFoundException;


public class Main {
    // Display main menu

    public static void displayMainMenu(Connection con) {
        while (true) {
        System.out.println("-----Main menu-----");
        System.out.println("What kinds of operations would you like to perform?");
        System.out.println("1. Operations for Administrator");
        System.out.println("2. Operations for Library User");
        System.out.println("3. Operations for Librarian");
        System.out.println("4. Exit this program");
        Scanner myObj = new Scanner(System.in);
        int choice;
        
            System.out.print("Enter Your Choice: ");
            choice = myObj.nextInt();
            System.out.println("");
        if (choice == 1) {
            displayAdminMenu(con);
        } else if (choice == 2) {
            displayLibraryUserMenu(con);
        } else if (choice == 3) {
            LibrarianMenu(con);
        } else if (choice == 4) {
            break;
        }
        }
    }

    // Display menu for administrator
    public static void displayAdminMenu(Connection con) {
        while (true) {
        System.out.println("-----Operations for administractor menu-----");
        System.out.println("What kinds of operations would you like to perform");
        System.out.println("1. Create all tables");
        System.out.println("2. Delete all tables");
        System.out.println("3. Load from datafile");
        System.out.println("4. Show number of records in each table");
        System.out.println("5. Return to the main menu");
        Scanner myObj = new Scanner(System.in);
        int choice;
        
            System.out.print("Enter Your Choice: ");
            choice = myObj.nextInt();
            System.out.println("");
            if (choice == 1) {
                createTable(con);
            } else if (choice == 2) {
                deleteTable(con);
            } else if (choice == 3) {
                loadData(con);
            } else if (choice == 4) {
                showNoOfRecord(con);
            } else if (choice == 5) break;
        } 
    }

    // Create all tables
    static void createTable(Connection conn) {
        try {
            String sql_user_Category = "CREATE TABLE IF NOT EXISTS user_category (ucid SMALLINT, max SMALLINT NOT NULL, period SMALLINT NOT NULL, PRIMARY KEY (ucid));";
            String sql_libuser = "CREATE TABLE IF NOT EXISTS libuser (libuid VARCHAR(10), name VARCHAR(25) NOT NULL, age SMALLINT NOT NULL, address VARCHAR(100) NOT NULL, ucid SMALLINT NOT NULL, PRIMARY KEY (libuid), FOREIGN KEY (ucid) REFERENCES user_category(ucid));";
            String sql_book_category = "CREATE TABLE IF NOT EXISTS book_category (bcid SMALLINT, bcname VARCHAR(30) NOT NULL, PRIMARY KEY (bcid));";
            String sql_book = "CREATE TABLE IF NOT EXISTS book (callnum VARCHAR(8), title VARCHAR(30) NOT NULL, publish DATE NOT NULL, rating DOUBLE, tborrowed SMALLINT NOT NULL, bcid SMALLINT NOT NULL, PRIMARY KEY (callnum), FOREIGN KEY (bcid) REFERENCES book_category(bcid));";
            String sql_copy = "CREATE TABLE IF NOT EXISTS copy (callnum VARCHAR(8), copynum SMALLINT, PRIMARY KEY (callnum, copynum), FOREIGN KEY (callnum) REFERENCES book(callnum));";
            String sql_borrow = "CREATE TABLE IF NOT EXISTS borrow (libuid VARCHAR(10), callnum VARCHAR(8), copynum SMALLINT, checkout DATE, `return` DATE, PRIMARY KEY (libuid, callnum, copynum, checkout), FOREIGN KEY (libuid) REFERENCES libuser(libuid), FOREIGN KEY (callnum, copynum) REFERENCES copy(callnum, copynum));";
            String sql_authorship = "CREATE TABLE IF NOT EXISTS authorship (aname VARCHAR(25), callnum VARCHAR(8), PRIMARY KEY (aname, callnum), FOREIGN KEY (callnum) REFERENCES book(callnum));";
            
            conn.prepareStatement(sql_user_Category).executeUpdate();
            conn.prepareStatement(sql_libuser).executeUpdate();
            conn.prepareStatement(sql_book_category).executeUpdate();
            conn.prepareStatement(sql_book).executeUpdate();
            conn.prepareStatement(sql_copy).executeUpdate();
            conn.prepareStatement(sql_borrow).executeUpdate();
            conn.prepareStatement(sql_authorship).executeUpdate();
            System.out.println("Processing...Done. Database is initialized.\n");
        } catch (Exception e) {
            System.out.println("Error: " + e);
            System.out.println("");
        }   
    }

    // Delete all tables
    static void deleteTable(Connection conn) {
        try {
            String[] tablesName = {"user_category", "libuser", "book_category", "book", "copy", "borrow", "authorship"};
            conn.prepareStatement("SET FOREIGN_KEY_CHECKS = 0").executeUpdate();
            for (int i = 0; i < tablesName.length; i++) {
                String sql = "DROP TABLE IF EXISTS " + tablesName[i] + ";";
                conn.prepareStatement(sql).executeUpdate();
            }
            conn.prepareStatement("SET FOREIGN_KEY_CHECKS = 1").executeUpdate();
            System.out.println("Processing...Done. Database is removed.\n");
        } catch (Exception e) {
            System.out.println("Error: " + e);
            System.out.println("");
        }
    }

    // Load from datafile
    static void loadData(Connection conn) {
        try {
            System.out.print("Type in the Source Data Folder Path: ");
            Scanner myObj = new Scanner(System.in);
            String path = myObj.nextLine();
            System.out.println("");
            loadDataToTable(conn, path+"/"+"user_category.txt", "user_category", "ucid, max, period", "(?, ?, ?)");
            loadDataToTable(conn, path+"/"+"user.txt", "libuser", "libuid, name, age, address, ucid", "(?, ?, ?, ?, ?)");
            loadDataToTable(conn, path+"/"+"book_category.txt", "book_category", "bcid, bcname", "(?, ?)");
            loadBookData(conn, path+"/"+"book.txt");
            loadBorrowData(conn, path+"/"+"check_out.txt");
            System.out.println("Processing...Done. Data is inputted to the database.\n");
        } catch (java.io.FileNotFoundException e) {
            System.out.println("Error: File Not Found\n");
        } catch (Exception e) {
            System.out.println("Error: " + e);
            System.out.println("");
        }
    }
    
    static void loadBorrowData(Connection conn, String path) throws Exception {
        File file = new File(path);
        Scanner obj = new Scanner(file);
        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO borrow (libuid, callnum, copynum, checkout, `return`) VALUES (?, ?, ?, ?, ?);");
        while (obj.hasNextLine()) {
            String data = obj.nextLine();
            String[] dataArray = data.split("\t");
            String checkoutDate = dataArray[3].substring(6, 10) + "-" + dataArray[3].substring(3, 5) + "-" + dataArray[3].substring(0, 2);

            pstmt.setString(1, dataArray[2]);
            pstmt.setString(2, dataArray[0]);
            pstmt.setString(3, dataArray[1]);
            pstmt.setDate(4, java.sql.Date.valueOf(checkoutDate));
            if (dataArray[4].equals("null")) {
                pstmt.setNull(5, java.sql.Types.NULL);
            } else {
                String returnDate = dataArray[4].substring(6, 10) + "-" + dataArray[4].substring(3, 5) + "-" + dataArray[4].substring(0, 2);
                pstmt.setDate(5, java.sql.Date.valueOf(returnDate));
            }
            pstmt.executeUpdate();
        }
    }

    
    static void loadBookData(Connection conn, String path) throws Exception {
        File file = new File(path);
        Scanner obj = new Scanner(file);
        PreparedStatement pstmt1a = conn.prepareStatement("INSERT INTO book (callnum, title, publish, rating, tborrowed, bcid) VALUES (?, ?, ?, ? ,?, ?);");
        PreparedStatement pstmt1b = conn.prepareStatement("INSERT INTO book (callnum, title, publish, rating, tborrowed, bcid) VALUES (?, ?, ?, NULL ,?, ?);");
        PreparedStatement pstmt2 = conn.prepareStatement("INSERT INTO copy (callnum, copynum) VALUES (?, ?);");
        PreparedStatement pstmt3 = conn.prepareStatement("INSERT INTO authorship (aname, callnum) VALUES (?, ?);");

        while (obj.hasNextLine()) {
            String data = obj.nextLine();
            String[] dataArray = data.split("\t");

            String callnum = dataArray[0];
            String title = dataArray[2];
            String date = dataArray[4].substring(6, 10) + "-" + dataArray[4].substring(3, 5) + "-" + dataArray[4].substring(0, 2);
            String tborrowed = dataArray[6];
            String bcid = dataArray[7];

            if (dataArray[5].equals("null")) {
                pstmt1b.setString(1, callnum);
                pstmt1b.setString(2, title);
                pstmt1b.setDate(3, java.sql.Date.valueOf(date));
                pstmt1b.setString(4, tborrowed);
                pstmt1b.setString(5, bcid);
                pstmt1b.executeUpdate();
            } else {
                pstmt1a.setString(1, callnum);
                pstmt1a.setString(2, title);
                pstmt1a.setDate(3, java.sql.Date.valueOf(date));
                pstmt1a.setDouble(4, Double.parseDouble(dataArray[5]));
                pstmt1a.setString(5, tborrowed);
                pstmt1a.setString(6, bcid);
                pstmt1a.executeUpdate();
            }
            

            String[] authorArray = dataArray[3].split(",");
            for (int j = 0; j < authorArray.length; j++) {
                pstmt3.setString(1, authorArray[j]);
                pstmt3.setString(2, dataArray[0]);
                pstmt3.executeUpdate();
            }
            
            for (int j = 0; j < Integer.parseInt(dataArray[1]); j++) {
                pstmt2.setString(1, dataArray[0]);
                pstmt2.setString(2, String.valueOf(j+1));
                pstmt2.executeUpdate();
            }
        }
    }

    static void loadDataToTable(Connection conn, String path, String table, String columnName, String val) throws Exception {
        File file = new File(path);
        Scanner obj = new Scanner(file);
        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + table + " (" + columnName + ") VALUES " + val);
        while (obj.hasNextLine()) {
            String data = obj.nextLine();
            String[] dataArray = data.split("\t");
            for (int i = 0; i < dataArray.length; i++) {
                pstmt.setString(i + 1, dataArray[i]);
            }
            pstmt.executeUpdate();
        }
    }

    // show number of records in each table
    static void showNoOfRecord(Connection conn) {
        String[] tablesName = {"user_category", "libuser", "book_category", "book", "copy", "borrow", "authorship"};
        System.out.println("Number of records in each table:");
        String sql = "SELECT COUNT(*) FROM ";
        try {
            for (int i = 0; i < tablesName.length; i++) {
                PreparedStatement pstmt = conn.prepareStatement(sql + tablesName[i]);
                ResultSet resultSet = pstmt.executeQuery();
                resultSet.next();
                System.out.println(tablesName[i] + ": " + resultSet.getInt("COUNT(*)"));
            }
            System.out.println("");
        } catch (Exception e) {
            System.out.println("Error: " + e);
            System.out.println("");
        }
    }

    // Display menu for library user
    static void displayLibraryUserMenu(Connection conn) {
        while (true) {
        System.out.println("-----Operations for library user menu-----");
        System.out.println("1. Search for Books");
        System.out.println("2. Show loan record of a user");
        System.out.println("3. Return to the main menu\n");
        Scanner myObj = new Scanner(System.in);
        int choice;
        
            System.out.print("Enter Your Choice: ");
            choice = myObj.nextInt();
            if (choice == 1) {
                searchBook(conn);
            } else if (choice == 2) {
                showLoan(conn);
            } else if (choice == 3)
            break;
            
        }
     System.out.println();

    }

    static void searchBook(Connection conn) {
        System.out.println("Choose the Search criterion:");
        System.out.println("1. call number");
        System.out.println("2. title");
        System.out.println("3. author");
        System.out.print("Enter Your Choice: ");
        Scanner myObj = new Scanner(System.in);
        String choice = myObj.nextLine();
        System.out.print("Type in the Search Keyword: ");
        String temp = myObj.nextLine();
        if (choice.equals("1")) {
            // search by call number
            try {
            String input_callnumber = temp;
            String temp1 = "create or replace view t2 as (SELECT callnum, count(*) as noofcopy from copy group by 1);";
            String temp2 = "create or replace view t1 as (SELECT callnum, count(*) as unreturned from borrow where `return` is null group by 1);";
            String sql_callnum = "select b.callnum, b.title, c.bcname, b.rating, t1.unreturned, t2.noofcopy,t2.noofcopy - IFNULL(t1.unreturned, 0) as available_books from book b left join book_category c on b.bcid = c.bcid left join t1 on b.callnum = t1.callnum left join t2 on b.callnum = t2.callnum where b.callnum = ? ORDER BY b.callnum;";
            String sql_callNumber_author = "SELECT authorship.aname from authorship, book WHERE book.callnum = ? and authorship.callnum = book.callnum ORDER BY book.callnum ASC;";
            conn.prepareStatement(temp1).executeUpdate();
            conn.prepareStatement(temp2).executeUpdate();
            PreparedStatement pstmt = conn.prepareStatement(sql_callnum);
            PreparedStatement pstmt2 = conn.prepareStatement(sql_callNumber_author);
            pstmt.setString(1, input_callnumber);
            pstmt2.setString(1, input_callnumber);
            ResultSet resultSet = pstmt.executeQuery();
            ResultSet resultSet2 = pstmt2.executeQuery();
            if (!resultSet.isBeforeFirst())
                System.out.println("No records found");
            else {
              System.out.println("|Call Num|Title|Book Category|Author|Rating|Available No. of Copy|");

                while (resultSet.next()) {
                  String name_of_authors = "";
                  System.out.print("|"+resultSet.getString("callnum")+"|"+resultSet.getString("title")+"|"+resultSet.getString("bcname")+"|");
                  while (resultSet2.next()) {
                    name_of_authors += resultSet2.getString("aname") + ", ";
                  }
                  System.out.print(name_of_authors.substring(0, name_of_authors.length()-2));
                  System.out.print("|"+resultSet.getString("rating")+"|");
                  System.out.println(resultSet.getInt("available_books")+"|");
                }
                System.out.println("End of query");
               

            }
            }
            catch (Exception e){
              System.out.println("Error: " + e);
            }
        } else if (choice.equals("2")) {
            // search by title
            try {
            String input_title = "%" + temp + "%";
            String temp1 = "create or replace view t2 as (SELECT callnum, count(*) as noofcopy from copy group by 1);";
            String temp2 = "create or replace view t1 as (SELECT callnum, count(*) as unreturned from borrow where `return` is null group by 1);";
            String sql_title = "select b.callnum, b.title, c.bcname, b.rating, t1.unreturned, t2.noofcopy,t2.noofcopy - IFNULL(t1.unreturned, 0) as available_books from book b left join book_category c on b.bcid = c.bcid left join t1 on b.callnum = t1.callnum left join t2 on b.callnum = t2.callnum where b.title like binary ? ORDER BY b.callnum asc;";
            String sql_callNumber_author = "SELECT authorship.aname from authorship, book WHERE book.callnum = ? and authorship.callnum = book.callnum ORDER BY book.callnum ASC;";
            conn.prepareStatement(temp1).executeUpdate();
            conn.prepareStatement(temp2).executeUpdate();
            PreparedStatement pstmt = conn.prepareStatement(sql_title);
            PreparedStatement pstmt2 = conn.prepareStatement(sql_callNumber_author);
            pstmt.setString(1, input_title);
            
            ResultSet resultSet = pstmt.executeQuery();
            if (!resultSet.isBeforeFirst())
                System.out.println("No records found");
            else {
              System.out.println("|Call Num|Title|Book Category|Author|Rating|Available No. of Copy|");
              
                while (resultSet.next()) {
                  String name_of_authors = "";
                  pstmt2.setString(1, resultSet.getString("callnum"));
                  ResultSet resultSet2 = pstmt2.executeQuery();
                  System.out.print("|"+resultSet.getString("callnum")+"|"+resultSet.getString("title")+"|"+resultSet.getString("bcname")+"|");
                  while (resultSet2.next()) {
                    name_of_authors += resultSet2.getString("aname") + ", ";
                  }
                  System.out.print(name_of_authors.substring(0, name_of_authors.length()-2));
                  System.out.print("|"+resultSet.getString("rating")+"|");
                  System.out.println(resultSet.getInt("available_books")+"|");
                }
                System.out.println("End of query");

                }
            }
            catch (Exception e){
              System.out.println("Error: " + e);
            }
        } else if (choice.equals("3")) {
            // search by author
            try {
            String input_author = "%" + temp + "%";
            String temp1 = "create or replace view t2 as (SELECT callnum, count(*) as noofcopy from copy group by 1);";
            String temp2 = "create or replace view t1 as (SELECT callnum, count(*) as unreturned from borrow where `return` is null group by 1);";
            String sql_author = "select b.callnum, b.title, c.bcname, IFNULL(b.rating, 0) as rating, t1.unreturned, t2.noofcopy,t2.noofcopy - IFNULL(t1.unreturned, 0) as available_books, a.aname from book b left join book_category c on b.bcid = c.bcid left join t1 on b.callnum = t1.callnum left join t2 on b.callnum = t2.callnum left join authorship a on a.callnum = b.callnum where a.aname like binary ? ORDER BY b.callnum asc;";
            String sql_callNumber_author = "SELECT authorship.aname from authorship, book WHERE book.callnum = ? and authorship.callnum = book.callnum ORDER BY book.callnum ASC;";
            conn.prepareStatement(temp1).executeUpdate();
            conn.prepareStatement(temp2).executeUpdate();
            PreparedStatement pstmt = conn.prepareStatement(sql_author);
            PreparedStatement pstmt2 = conn.prepareStatement(sql_callNumber_author);
            pstmt.setString(1, input_author);
            
            ResultSet resultSet = pstmt.executeQuery();
            
            if (!resultSet.isBeforeFirst())
                System.out.println("No records found");
            else {
              System.out.println("|Call Num|Title|Book Category|Author|Rating|Available No. of Copy|");
                while (resultSet.next()) {
                  String name_of_authors = "";
                  pstmt2.setString(1, resultSet.getString("callnum"));
                  ResultSet resultSet2 = pstmt2.executeQuery();
                  System.out.print("|"+resultSet.getString("callnum")+"|"+resultSet.getString("title")+"|"+resultSet.getString("bcname")+"|");
                  while (resultSet2.next()) {
                    name_of_authors += resultSet2.getString("aname") + ", ";
                  }
                  System.out.print(name_of_authors.substring(0, name_of_authors.length()-2));
                  System.out.print("|"+resultSet.getString("rating")+"|");
                  System.out.println(resultSet.getInt("available_books")+"|");
                }
                System.out.println("End of query");
            }
            }
            catch (Exception e){
              System.out.println("Error: " + e);
            }
        }
         System.out.println();
    }

    static void showLoan(Connection conn) {
        System.out.print("Enter The User ID: ");
        Scanner myObj = new Scanner(System.in);
        String input_userID = myObj.nextLine();
        try {
            String sql_loan = "SELECT book.callnum, copy.copynum, book.title,borrow.checkout, IFNULL(borrow.`return`,\"NULL\") as return2,libuser.ucid FROM book, copy,libuser,borrow WHERE libuser.libuid = ? and libuser.libuid = borrow.libuid and book.callnum = copy.callnum and copy.copynum = borrow.copynum and borrow.callnum = copy.callnum ORDER BY book.callnum ASC;";
            String sql_callNumber_author = "SELECT authorship.aname from authorship, book WHERE book.callnum = ? and authorship.callnum = book.callnum ORDER BY book.callnum ASC;";
            PreparedStatement pstmt = conn.prepareStatement(sql_loan);
            PreparedStatement pstmt2 = conn.prepareStatement(sql_callNumber_author);
            pstmt.setString(1, input_userID);
            ResultSet resultSet = pstmt.executeQuery();

            if (!resultSet.isBeforeFirst())
                System.out.println("No records found");
            else {
              System.out.println("|CallNum|CopyNum|Title|Author|Check-out|Returned?|");

                while (resultSet.next()) {
                  String name_of_authors = "";
                  pstmt2.setString(1, resultSet.getString("callnum"));
                  ResultSet resultSet2 = pstmt2.executeQuery();
                  System.out.print("|"+resultSet.getString("callnum")+"|"+resultSet.getString("copynum") + "|" + resultSet.getString("title")+"|");
                  while (resultSet2.next()) {
                    name_of_authors += resultSet2.getString("aname") + ", ";
                  }
                  System.out.print(name_of_authors.substring(0, name_of_authors.length()-2));
                  System.out.print("|" + resultSet.getString("checkout")+"|");
                  if (resultSet.getString("return2").equals("NULL")) System.out.println("No|"); else System.out.println("Yes|");
                }
            System.out.println("End of Query");
        }
        }
        catch (Exception e){
          System.out.println("Error: " + e);
        }
         System.out.println();
    }
  

    // Display menu for librarian
    public static void LibrarianMenu(Connection con) {
        while (true) {
        System.out.println("-----Operations for librarian menu-----");
        System.out.println("1. Book Borrowing");
        System.out.println("2. Book Returning");
        System.out.println("3. List all un-returned book copies which are checked-out within a period");
        System.out.println("4. Return to the main menu");
        int libchoice;
        Scanner myObj = new Scanner(System.in);
            System.out.print("Enter Your Choice: ");
            libchoice = myObj.nextInt();
            if (libchoice == 1) {
                borrowBook(con);
            } else if (libchoice == 2) {
                returnBook(con);
            } else if (libchoice == 3) {
                listUnreturn(con);
            } else if (libchoice == 4) break;
        }
        System.out.println();
    }

    // Display Librarian borrowbook
    public static void borrowBook(Connection con) {
        System.out.print("Enter The User ID: ");
        Scanner myObj = new Scanner(System.in);
        String uid = myObj.nextLine();
        System.out.print("Enter The Call Number: ");
        String callnum = myObj.nextLine();
        System.out.print("Enter The Copy Number: ");
        int copynum = myObj.nextInt();
        // sql statm
        String sqlstatm,sql;
        PreparedStatement pstatm,pstatm1;
        try {
            sqlstatm = "SELECT * FROM " + "borrow WHERE " + "callnum = ? AND " + "copynum = ? AND " + "`return` IS NULL ;";
            pstatm = con.prepareStatement(sqlstatm);
            pstatm.setString(1, callnum);
            pstatm.setInt(2, copynum);
            ResultSet rs = pstatm.executeQuery();
              if (!rs.next()) {
                String sqlborrow;
                PreparedStatement pborrow;
                /* Insert record */
                sqlborrow = "INSERT INTO borrow (callnum, copynum, libuid, checkout, `return`) VALUES (?, ?, ?, CURDATE(), NULL)";
                pborrow = con.prepareStatement(sqlborrow);
                pborrow.setString(1, callnum);
                pborrow.setInt(2, copynum);
                pborrow.setString(3, uid);

                /* execute SQL */
                pborrow.execute();
                System.out.println("Book borrowing performed successfully.\n");
              } else {
                /* already borrow */
                System.out.print("error:The Book (callnum: " + callnum + " ,copynum: " + copynum  + ") has been borrowed!");
                System.out.println(" / Someone didn't return this book\n");
            }
        }catch (java.sql.SQLIntegrityConstraintViolationException ex) {
            System.out.println("error: Invalid user input!!\n");
        }catch (Exception e) {
            System.out.println("failed to borrow book!!");
            System.out.println("error: " + e + "\n");
        }
    }

    public static void returnBook(Connection con) {
      System.out.print("Enter The User ID: ");
      Scanner myObj = new Scanner(System.in);
      String uid = myObj.nextLine();
      System.out.print("Enter The Call Number: ");
      String callnum = myObj.nextLine();
      System.out.print("Enter The Copy Number: ");
      int copynum = myObj.nextInt();
      System.out.print("Enter Your Rating of The Book: ");
      Double rating = myObj.nextDouble();
        // sql statm
      String sqlstatm;
      PreparedStatement pstatm;

        try {
            sqlstatm = "SELECT * FROM  borrow WHERE libuid = ? AND callnum = ? AND copynum = ? AND `return` IS NULL;";
            pstatm = con.prepareStatement(sqlstatm);
            pstatm.setString(1, uid);
            pstatm.setString(2, callnum);
            pstatm.setInt(3, copynum);
            ResultSet rs = pstatm.executeQuery();
              if (rs.next()) {
                /* Return the book */
                String sqlreturn;
                PreparedStatement preturn;
                String sqlrating;
                PreparedStatement prating;
                String sqltborrow;
                PreparedStatement ptborrow;
                /* Update record */
                sqlreturn = "UPDATE borrow SET `return` = CURDATE()  WHERE libuid = ? AND callnum = ? AND copynum = ?;";
                preturn = con.prepareStatement(sqlreturn);
                preturn.setString(1, uid);
                preturn.setString(2, callnum);
                preturn.setInt(3, copynum);
                
                sqlrating = "UPDATE book SET rating = (rating * tborrowed +?)/(tborrowed+1)  WHERE callnum = ?;";
                prating = con.prepareStatement(sqlrating);
                prating.setDouble(1, rating);
                prating.setString(2, callnum);

                sqltborrow = "UPDATE book SET tborrowed=tborrowed+1  WHERE callnum = ?;";
                ptborrow = con.prepareStatement(sqltborrow);
                ptborrow.setString(1, callnum);

                /* execute SQL */
                preturn.execute();
                prating.execute();
                ptborrow.execute();
                System.out.println("Book returning performed successfully.\n");
                } else {
                /* already return */
                  System.out.println("failed to return book!!\n");   
                }
          } catch (Exception e) {
            System.out.println("failed to return book!!");
            System.out.println("error: " + e + "\n");
        }
    }

    public static void listUnreturn(Connection con) {
        System.out.print("Type in the starting date[dd/mm/yyyy]: ");
        Scanner myObj = new Scanner(System.in);
        String sdate = myObj.nextLine();
        System.out.print("Type in the ending date[dd/mm/yyyy]: ");
        String edate = myObj.nextLine();
        // sql statm
        String[] data1 = sdate.split("/");
        String[] data2 = edate.split("/");
        if(data1.length != 3 || data2.length != 3){
          System.out.println("error: Invalid user input!!\n");
          return;
        }
        String startDate = data1[2] + "-" + data1[1] + "-" + data1[0];
        String endDate = data2[2] + "-" + data2[1] + "-" + data2[0];
        String sqlunreturn;
        PreparedStatement punreturn;
        boolean resultExist=false;
        if( data1[2].length() !=4 ||  data2[2].length() !=4){
          System.out.println("error: Invalid user input!!\n");
          return;
        }
        System.out.print("List of UnReturned Book:\n");
        try {

           
          sqlunreturn = "SELECT libuid, callnum, copynum, checkout FROM borrow WHERE `return` IS NULL AND checkout BETWEEN ? AND ? ORDER BY checkout DESC;";
          punreturn = con.prepareStatement(sqlunreturn);
          punreturn.setString(1, startDate);
          punreturn.setString(2, endDate);
          ResultSet unreturn = punreturn.executeQuery();
          System.out.println("|LibUID|CallNum|CopyNum|Checkout|");
          while(unreturn.next()){
            resultExist=true;
            System.out.println("|"+unreturn.getString("libuid")+"|"+unreturn.getString("callnum")+"|"+unreturn.getString("copynum")+"|"+unreturn.getString("checkout")+"|");
          }
          if(!resultExist){
            System.out.println("No matching record!");
          }
          System.out.println("End of Query\n");
        }catch (Exception e) {
          System.out.println("Error: " + e + "\n");
        } 
    }

    public static void main(String[] args) {
        // Parameters
        String dbAddress = "jdbc:mysql://projgw.cse.cuhk.edu.hk:2633/db28";
        String dbUsername = "Group28";
        String dbPassword = "CSCI3170";

        // Create connection
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection(dbAddress, dbUsername, dbPassword);
            System.out.println("Welcome to Library Inquiry System!\n");
            displayMainMenu(con);
            con.close();
        } catch (ClassNotFoundException e) {
            System.out.println("Class not found");
            System.exit(0);
        } catch (SQLException e) {
            System.out.println(e);
        }
    }
}
                  