import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {
    public static void main(String[] args) throws IOException {
        String info = "\nHello! This is the manager of stores and products."
                + "\nThe following commands are possible (example):"
                + "\n1.Add a store (1 name_store)"
                + "\n2.Add a product (2 name_product)"
                + "\n3.Post the product in the store (3 name_product name_store)"
                + "\n4.Request statistics (4)"
                + "\n5.Information"
                + "\n6.Exit";

        DB_Manager db_manager = new DB_Manager();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        System.out.println(info);

        while (true) {
            String line = reader.readLine();
            String[] inputConsole = line.split("\\s+");
            if (inputConsole[0].equals("6")) {
                System.out.println("Goodbye!");
                break;
            }
            try {
                switch (inputConsole[0]) {
                    case "1":
                        db_manager.addShop(inputConsole[1]);
                        break;
                    case "2":
                        db_manager.addProduct(inputConsole[1], Integer.parseInt(inputConsole[2]));
                        break;
                    case "3":
                        db_manager.postProduct(inputConsole[1], inputConsole[2]);
                        break;
                    case "4":
                        db_manager.getStatistics();
                        break;
                    case "5":
                        System.out.println(info);
                        break;
                    default:
                        throw new IllegalArgumentException("Не верная команда");

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
