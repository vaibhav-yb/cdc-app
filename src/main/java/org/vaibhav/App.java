package org.vaibhav;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class App {
  private static long iterations = 0;

  private static void runWorkload(String endpoint) throws Exception {
    String ybUrl = "jdbc:yugabytedb://" + endpoint + ":5433/yugabyte?" +
      "user=yugabyte&password=yugabyte";
    Connection conn = DriverManager.getConnection(ybUrl);
    Statement st = conn.createStatement();

    /*
      CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT);
      CREATE TABLE employee (id INT PRIMARY KEY, emp_name TEXT, d_id INT, FOREIGN KEY (d_id) REFERENCES department(id));
      CREATE TABLE contract (id INT PRIMARY KEY, contract_name TEXT, c_id INT, FOREIGN KEY (c_id) REFERENCES employee(id));
      CREATE TABLE address (id INT PRIMARY KEY, area_name TEXT, a_id INT, FOREIGN KEY (a_id) REFERENCES contract(id));
      CREATE TABLE locality (id INT PRIMARY KEY, loc_name TEXT, l_id INT, FOREIGN KEY (l_id) REFERENCES address(id));
     */

    while(true) {
      ++iterations;
      System.out.println("Starting iteration " + iterations);
      // If this test needs to be run more for higher duration, this scale factor can be changed
      // accordingly.
      final int scaleFactor = 1;
      final int iterations = 5 * scaleFactor;
      int departmentId = 1;
      int employeeId = 1, employeeBatchSize = 5 * scaleFactor;
      int contractId = 1, contractBatchSize = 6 * scaleFactor;
      int addressId = 1, addressBatchSize = 7 * scaleFactor;
      int localityId = 1, localityBatchSize = 8 * scaleFactor;

      // Lists to store the expected indices of the elements of respective tables in the final
      // list of messages we will be receiving after streaming.
//      List<Integer> departmentIndices = new ArrayList<>();
//      List<Integer> employeeIndices = new ArrayList<>();
//      List<Integer> contractIndices = new ArrayList<>();
//      List<Integer> addressIndices = new ArrayList<>();
//      List<Integer> localityIndices = new ArrayList<>();

      for (long i = 0; ; ++i) {
        long totalCount = 0;
        st.execute(String.format("INSERT INTO department VALUES (%d, 'my department no %d');", departmentId, departmentId));

        // Inserting the index of the record for department table at its appropriate position.
//        departmentIndices.add((int) totalCount);
        ++totalCount;

        for (int j = employeeId; j <= employeeId + employeeBatchSize - 1; ++j) {
          System.out.println("inserting into employee with id " + j);
          st.execute(String.format("BEGIN; INSERT INTO employee VALUES (%d, 'emp no %d', %d); COMMIT;", j, j, departmentId));
//          employeeIndices.add((int) totalCount);
          ++totalCount;
          for (int k = contractId; k <= contractId + contractBatchSize - 1; ++k) {
            System.out.println("inserting into contract with id " + k);
            st.execute(String.format("BEGIN; INSERT INTO contract VALUES (%d, 'contract no %d', %d); COMMIT;", k, k, j /* employee fKey */));
//            contractIndices.add((int) totalCount);
            ++totalCount;

            for (int l = addressId; l <= addressId + addressBatchSize - 1; ++l) {
              System.out.println("inserting into address with id " + l);
              st.execute(String.format("BEGIN; INSERT INTO address VALUES (%d, 'address no %d', %d); COMMIT;", l, l, k /* contract fKey */));
//              addressIndices.add((int) totalCount);
              ++totalCount;

              for (int m = localityId; m <= localityId + localityBatchSize - 1; ++m) {
                System.out.println("inserting into locality with id " + m);
                st.execute(String.format("BEGIN; INSERT INTO locality VALUES (%d, 'locality no %d', %d); COMMIT;", m, m, l /* address fKey */));
//                localityIndices.add((int) totalCount);
                ++totalCount;
              }
              // Increment localityId for next iteration.
              localityId += localityBatchSize;
            }
            // Increment addressId for next iteration.
            addressId += addressBatchSize;
          }
          // Increment contractId for next iteration.
          contractId += contractBatchSize;
        }

        // Increment employeeId for the next iteration
        employeeId += employeeBatchSize;

        // Increment department ID for more iterations
        ++departmentId;

        System.out.println("Total records inserted in iteration " + iterations + ": " + totalCount);
      }
    }
  }

  public static void main(String[] args) {
    // args will contain the endpoints
    if (args.length == 0) {
      System.out.println("No endpoints specified to connect to, exiting...");
      System.exit(-1);
    }

    int index = 0;
    while (true) {
      try {
        runWorkload(args[index]);
      } catch (Exception e) {
        System.out.println("Exception caught: " + e);
        System.out.println("Trying again...");
        ++index;
        if (index >= args.length) {
          index = 0;
        }
      }
    }
  }
}
