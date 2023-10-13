package com.jspiders.Jdbc.Operation;
import  java.util.Properties;
import java.util.Scanner;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
public class JdbcOperation1 {
	private static Connection connection;
	private static Statement statement;
	private static int Result;
	private static ResultSet resultSet;
	private static Properties properties=new Properties();
	private static FileInputStream file;
	private static String filepath="C:\\Users\\pagas\\weje2\\Jdbc\\src\\Resources\\db_info.properties";
	private static String query;
	public static void main(String[] args) {
		//1.open connection
		try {
			OpenConnection();
			query="CREATE TABLE emp (" +
	                "empno INT(3) PRIMARY KEY," +
	                "ename VARCHAR(50) NOT NULL," +
	                "job VARCHAR(50)," +
	                "mgr INT," +
	                "hiredate DATE NOT NULL," +
	                "sal DECIMAL(10, 2) NOT NULL," +
	                "comm DECIMAL(10, 2)" +
	                ")";
			Result=statement.executeUpdate(query);
			System.out.println("Query ok " +Result+"no of row(s)affected");
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			closeConnection();
		}
		//2.insert 14 records
		try {
			OpenConnection();
			Scanner scanner=new Scanner(System.in);
			for (int i = 0; i <=14; i++) {
				System.out.println("enter the id");
				int empno=scanner.nextInt();
				System.out.println("enter the employee name");
				String ename=scanner.next();
				System.out.println("Enter the job");
				String job=scanner.next();
				System.out.println("enter mgr no");
				int mgr=scanner.nextInt();
				System.out.println("enter the hiredate");
				String hiredate=scanner.next();
				System.out.println("enter the sal");
				long sal=scanner.nextLong();
				System.out.println("comm");
				long comm=scanner.nextLong();
				
				query="INSERT INTO emp "+
				"VALUES("+ empno + ", '"+ ename+"', '"+job+"', " +mgr+", '"+hiredate+"',"+sal +","+ comm+")";
				Result=Result+statement.executeUpdate(query);
				System.out.println("query ok"+Result+"rows(s) affected");	
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			closeConnection();
			//scanner.close();
		}
		//3.display all records
		try {
			OpenConnection();
				query="select * from emp";
						resultSet=statement.executeQuery(query);
						while (resultSet.next()) {
							System.out.println(resultSet.getString(1)+"|"+resultSet.getString(2)+"|"+resultSet.getString(3)+"|"+resultSet.getString(4)
									            );
						}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			closeConnection();
		}
		//4 update 2 records
		try {
			OpenConnection();
			query="update emp set sal = sal + 500 "
			+"where empno in (123,456)";
			Result=statement.executeUpdate(query);
			if(Result !=0) {
				System.out.println("query ok "+Result+"no of row(s) affected");
			}
		} catch (Exception e) {
			e.printStackTrace();
	        
			
		}finally {
			 closeConnection();
		}
		//show all records
		try {
			OpenConnection();
				query="select * from emp";
						resultSet=statement.executeQuery(query);
						while (resultSet.next()) {
							System.out.println(resultSet.getString(1)+"|"+resultSet.getString(2)+"|"+resultSet.getString(3)+"|"+resultSet.getString(4)
									            );
						}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			closeConnection();
		}
		//5. delete 3 records
		try {
			OpenConnection();
			query="delete from emp where empno in (123,456,101)";
			Result=statement.executeUpdate(query);
			System.out.println("query ok"+Result+"row(s) affected");
			
		} catch (Exception e) {
			e.printStackTrace();
			
		}finally {
			closeConnection();
		}
	}
	
	

	
		
		private static void OpenConnection() {
		try {
			file=new FileInputStream(filepath);
			properties.load(file);
			Class.forName(properties.getProperty("driverpath"));
			connection=DriverManager.getConnection(properties.getProperty("dburl"),properties);
			statement=connection.createStatement();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	private static void closeConnection() {
		try {
			if (connection !=null) {
				connection.close();
				
			}if (statement !=null) {
				statement.close();
				
			}
			if (resultSet !=null) {
				resultSet.close();
			}
			if (file !=null) {
				file.close();	
			}
			if (Result !=0) {
				Result=0;
				
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			
		}
		
	}
	
	

}
