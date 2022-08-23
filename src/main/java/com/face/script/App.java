package com.face.script;

import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.simple.JSONObject;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import io.github.cdimascio.dotenv.Dotenv;

public class App {
    static Dotenv dotenv = Dotenv.load();
    static long startTime = System.currentTimeMillis();
    static Connection con;
    static Map<String, double[]> customerDataArray = new HashMap<>();
    static List<String> nullData = new ArrayList<>();
    static List<String> cannotEncode = new ArrayList<>();
    
    static final String authToken = "Basic " + Base64.getEncoder().encodeToString(
            (dotenv.get("elastic_user") + ":" + dotenv.get("elastic_password")).getBytes());

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, UnirestException {

        Class.forName("oracle.jdbc.OracleDriver");
        con = DriverManager.getConnection(
                dotenv.get("db_url"), dotenv.get("db_user"), dotenv.get("db_password"));

        List<String> custNumbers = new ArrayList<>();
        custNumbers = getCustNumFromDatabase();

        for (int i = 0; i < custNumbers.size(); i++) {
            encodeAndInsertToES(custNumbers, i);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Successful : " + customerDataArray.size());
        System.out.println("No data: " + nullData.size());
        System.out.println("Unable to Encode: " + cannotEncode.size());
        System.out.println("Time taken: " + (endTime - startTime) / 1000 + "sec");
    }

    static double[] convertToArray(JSONArray jsonArray) {

        double[] fData = new double[jsonArray.length()];
        for (int i = 0; i < jsonArray.length(); i++) {
            try {
                fData[i] = jsonArray.getDouble(i);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        return fData;
    }

    public static byte[] getByteDataFromBlob(Blob blob) {
        if (blob != null) {
            try {
                return blob.getBytes(1, (int) blob.length());
            } catch (SQLException ex) {
                System.out.println(ex);
            }
        }
        return null;
    }

    public static List<String> getCustNumFromDatabase() throws SQLException, ClassNotFoundException {
        List<String> custNumbers = new ArrayList<>();
        java.sql.Statement stmt = con.createStatement();
        ResultSet rs = ((java.sql.Statement) stmt).executeQuery(dotenv.get("db_query"));

        while (rs.next()) {
            custNumbers.add(rs.getString("CUST_NO"));
        }
        return custNumbers;

    }

    public static Void encodeAndInsertToES(List<String> custNumbers, int i)
            throws SQLException, ClassNotFoundException {
        PreparedStatement smt = (PreparedStatement) con
                .prepareStatement("select CUST_IMAGE from MEMBER_IMAGE where CUST_NO= ? ");
        ((PreparedStatement) smt).setString(1, custNumbers.get(i));
        ResultSet rs = ((PreparedStatement) smt).executeQuery();

        while (rs.next()) {
            try {
                String base64 = Base64.getEncoder().encodeToString(getByteDataFromBlob(rs.getBlob("CUST_IMAGE")));
                JSONObject obj = new JSONObject();
                obj.put("image", base64);

                try {
                    HttpResponse<JsonNode> response = Unirest.post(dotenv.get("api_url"))
                            .header("Content-Type", "application/json")
                            .body(obj.toJSONString())
                            .asJson();

                    JSONArray encodings = new JSONArray();
                    encodings = response.getBody().getObject().getJSONObject("data").getJSONArray("encodings");
                    double imageData[];
                    imageData = convertToArray(encodings);
                    customerDataArray.put(custNumbers.get(i), imageData);
                    JSONObject custObj = new JSONObject();
                    custObj.put("custNum", custNumbers.get(i));
                    custObj.put("faceData", new JSONArray(imageData));

                    try {
                        kong.unirest.Unirest.config().verifySsl(false);
                        kong.unirest.Unirest.post(dotenv.get("elasitc_api"))
                                .header("Authorization", authToken)
                                .header("Content-Type", "application/json")
                                .body(custObj.toJSONString())
                                .asString();

                        System.out.println("data inserted for: " + custNumbers.get(i));
                    } catch (Exception e) {
                        System.out.println("Exception.........");
                        e.printStackTrace();
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                    cannotEncode.add(custNumbers.get(i));
                }

            } catch (Exception ex) {
                System.out.println("[ERROR]" + ex.toString());
                nullData.add(custNumbers.get(i));
            } 
        }
        rs.close();
        smt.close();


        return null;

    }

}
