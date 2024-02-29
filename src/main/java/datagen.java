import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class datagen {
    public static void main(String[] args) {
        generateCustomers("data/customers.txt", 50000);
        generatePurchases("data/purchases.txt", 5000000);
    }

    public static void generateCustomers(String fileName, int numRows) {
        Random random = new Random();
        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
            for (int i = 1; i <= numRows; i++) {
                int id = i;
                String name = generateString(11, 10);
                int age = random.nextInt(83) + 18; // Random number between 18 and 100
                int countryCode = random.nextInt(500) + 1; // Random number between 1 and 500
                float salary = random.nextFloat() * 9999900 + 100; // Random number between 100 and 10,000,000
                writer.println(id + "," + name + "," + age + "," + countryCode + "," + salary);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void generatePurchases(String fileName, int numRows) {
        Random random = new Random();
        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
            for (int i = 1; i <= numRows; i++) {
                int transID = i;
                int custID = random.nextInt(50000) + 1; // Random customer ID between 1 and 50,000
                float transTotal = random.nextFloat() * 1990 + 10; // Random number between 10 and 2000
                int transNumItems = random.nextInt(15) + 1; // Random number of items between 1 and 15
                String transDesc = generateString(31, 20);
                writer.println(transID + "," + custID + "," + transTotal + "," + transNumItems + "," + transDesc);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static String generateString(int high, int low) {
        Random random = new Random();
        int length = random.nextInt(high) + low; // Random length between 10 and 20
        StringBuilder name = new StringBuilder();
        for (int i = 0; i < length; i++) {
            char c = (char) (random.nextInt(26) + 'a'); // Random lowercase character
            name.append(c);
        }
        return name.toString();
    }

}
