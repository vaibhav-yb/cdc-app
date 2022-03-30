package org.vaibhav;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ApiSubAppleOrigTransactions implements Runnable {
    long i;
    int idx = 0;

    String[] endpoint;
    private Connection conn;
    private Statement st;

    long min;
    long max;

    ApiSubAppleOrigTransactions(String[] endpointArray, long min, long max) {
        this.endpoint = endpointArray;
        this.i = min;
        this.min = min;
        this.max = max;
    }

    @Override public void run() {
        try {
            conn = DriverManager.getConnection("jdbc:yugabytedb://" + endpoint[idx] + ":5433/yugabyte?" +
                    "user=yugabyte&password=yugabyte");
            st = conn.createStatement();
        } catch (SQLException se) {
            System.out.println("Exeption while creating connection: " + se);
            return;
        }
        while (true) {
            try {
                // Insert in loop here
                for (; i > 0; ++i) {
                    st.executeUpdate(String.format(UtilStrings.apiSubAppleOrigTransactions, i));
                    if (i % 1000 == 0) {
                        System.out.println("Total rows written to api_sub_apple_orig_transactions --> " + i);
                    }
                }
            }
            catch (Exception e) {
                System.out.println("Exception thrown in thread (" + endpoint[idx] + "): " + ApiSubAppleOrigTransactions.class.getName() + " --> " + e);

                ++idx;
                if (idx >= endpoint.length) {
                    idx = 0;
                }

                try {
                    st.close();
                    conn.close();
                    int count = 0;
                    while (count < 3){
                        conn = DriverManager.getConnection("jdbc:yugabytedb://" + endpoint[idx] + ":5433/yugabyte?" +
                                "user=yugabyte&password=yugabyte");
                        st = conn.createStatement();
                        count++;
                    }
                } catch (SQLException se) {
                    System.out.println("Exception while closing and recreating connection...");
                    return;
                }
            }
        }
    }
}
