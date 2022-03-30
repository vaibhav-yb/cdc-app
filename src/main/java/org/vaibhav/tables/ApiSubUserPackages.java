package org.vaibhav.tables;

import org.vaibhav.UtilStrings;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ApiSubUserPackages implements Runnable {
    long i;
    int idx = 0;

    private String[] endpoint;
    private Connection conn;
    private Statement st;
    //int multiplier = 1;

    private long min;
    private long max; // max will be excluded from being inserted;

    public ApiSubUserPackages(String[] endpointArray, long min, long max) {
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
        while (i < max) { // set this to while (true) -1, -2, -3, -4, -5 --> 1
            try {
                // Insert in loop here
                for (; i < max; ++i) {
                    st.executeUpdate(String.format(UtilStrings.apiSubUserPackages, i));
                    if (i % 1000 == 0) {
                        System.out.println("Total rows written to api_sub_user_packages --> " + i);
                    }
                    /*
                        if (i == max) { i = min; multiplier = -1 * multiplier; }
                        int multiplier
                    * */
                }
            }
            catch (Exception e) {
                System.out.println("Exception thrown in thread (" + endpoint[idx] + "): " + ApiSubUserPackages.class.getName() + " --> " + e);

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
