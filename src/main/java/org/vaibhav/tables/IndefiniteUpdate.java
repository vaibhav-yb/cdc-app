package org.vaibhav.tables;

import org.vaibhav.UtilStrings;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class IndefiniteUpdate implements Runnable {
    long i = 1;
    int idx = 0;
    long count = 0;
    long min;
    long max;
    long valueForUpdate;

    private String endpoint;
    private Connection conn;
    private Statement st;



    public IndefiniteUpdate(String endpoint, long min, long max) {
        this.endpoint = endpoint;
        this.min = min;
        this.max = max;
        this.valueForUpdate = min;
    }

    @Override public void run() {
        try {
            conn = DriverManager.getConnection("jdbc:yugabytedb://" + endpoint + ":5433/yugabyte?" +
                    "user=yugabyte&password=yugabyte");
            st = conn.createStatement();
        } catch (SQLException se) {
            System.out.println("Exeption while creating connection: " + se);
            return;
        }
        while (valueForUpdate <= max) {
            try {
                // Insert in loop here
                for (; valueForUpdate <= max; ++count) {
                    System.out.println(String.format("Executing: UPDATE test_app SET v = %d WHERE k = 1;", valueForUpdate));
                    st.execute("BEGIN;");
                    st.executeUpdate(String.format("UPDATE test_app SET v = %d WHERE k = 1;", valueForUpdate));
                    st.execute("COMMIT;");
                    ++valueForUpdate;
//                    System.out.println("Total rows updated in test_app --> " + count);
                }
            }
            catch (Exception e) {
                System.out.println("Exception thrown in thread (" + endpoint + "): " + IndefiniteUpdate.class.getName() + " --> " + e);

//                ++idx;
//                if (idx >= endpoint.length) {
//                    idx = 0;
//                }

                try {
                    st.close();
                    conn.close();
                    int tryCount = 0;
                    while (tryCount < 3){
                        conn = DriverManager.getConnection("jdbc:yugabytedb://" + endpoint + ":5433/yugabyte?" +
                                "user=yugabyte&password=yugabyte");
                        st = conn.createStatement();
                        tryCount++;
                    }
                } catch (SQLException se) {
                    System.out.println("Exception while closing and recreating connection...");
                    return;
                }
            }
        }
    }
}
