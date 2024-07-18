package main.java;

import javax.crypto.spec.PSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;

public class JDBCRunner {
    static Scanner sc = new Scanner(System.in);

    private static final String PROTOCOL = "jdbc:postgresql://";
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String URL_LOCALE_NAME = "localhost/";

    private static final String DATABASE_NAME = "postgres";

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";
    public static final String DATABASE_PASS = "postgres";
    public static ArrayList<Integer> idNumberTable;

    public static void main(String[] args) {

        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            while (true) {
                System.out.println("Select an option:");
                System.out.println("1. Get Cars");
                System.out.println("2. Get Drivers");
                System.out.println("3. Get Fines");
                System.out.println("4. Get Fines by Car ID");
                System.out.println("5. Get Car by Driver ID");
                System.out.println("6. Add Car");
                System.out.println("7. Add Driver");
                System.out.println("8. Add Fine");
                System.out.println("9. Update Car Fines");
                System.out.println("10. Remove Car");
                System.out.println("11. Remove Driver");
                System.out.println("12. Remove Fines");
                System.out.println("13. Get Experienced Drivers");
                System.out.println("14. Get with fine");
                System.out.println("0. Exit");

                int choice = sc.nextInt();
                sc.nextLine(); // consume newline

                switch (choice) {
                    case 1:
                        getWithNumber(connection);
                        break;
                    case 2:
                        getDriver(connection);
                        break;
                    case 3:
                        getFines(connection);
                        break;
                    case 4:
                        System.out.print("Enter Car ID: ");
                        int carId = sc.nextInt();
                        getFinesCar(connection, carId);
                        break;
                    case 5:
                        System.out.print("Enter Driver ID: ");
                        int driverId = sc.nextInt();
                        getCarDriver(connection, driverId);
                        break;
                    case 6:
                        System.out.print("Enter Car Number: ");
                        String car_number = sc.nextLine();
                        System.out.print("Enter Brand: ");
                        String brand = sc.nextLine();
                        System.out.print("Enter Model: ");
                        String model = sc.nextLine();
                        System.out.print("Enter Fines: ");
                        String fines = sc.nextLine();
                        System.out.print("Enter Owner: ");
                        String owner = sc.nextLine();
                        addNumber(connection, car_number, brand, model, fines, owner);
                        break;
                    case 7:
                        System.out.print("Enter First Name: ");
                        String first_name = sc.nextLine();
                        System.out.print("Enter Last Name: ");
                        String last_name = sc.nextLine();
                        System.out.print("Enter Patronymic: ");
                        String patronymic = sc.nextLine();
                        System.out.print("Enter Age: ");
                        int age = sc.nextInt();
                        System.out.print("Enter Experience: ");
                        int experience = sc.nextInt();
                        System.out.print("Enter License Number: ");
                        int license_number = sc.nextInt();
                        System.out.print("Enter Driver ID: ");
                        int driver_id = sc.nextInt();
                        addDriver(connection, first_name, last_name, patronymic, age, experience, license_number, driver_id);
                        break;
                    case 8:
                        System.out.print("Enter Car ID: ");
                        int fineCarId = sc.nextInt();
                        sc.nextLine();
                        System.out.print("Enter Car Number: ");
                        String fineCarNumber = sc.nextLine();
                        System.out.print("Enter Type: ");
                        String type = sc.nextLine();
                        System.out.print("Enter Cost: ");
                        int cost = sc.nextInt();
                        addFines(connection, fineCarId, fineCarNumber, type, cost);
                        break;
                    case 9:
                        getWithNumber(connection);
                        System.out.print("Enter Car ID: ");
                        int updateCarId = sc.nextInt();
                        sc.nextLine();
                        System.out.print("Enter Fines: ");
                        String updateFines = sc.nextLine();
                        correctCar(connection, updateCarId, updateFines);
                        break;
                    case 10:
                        System.out.print("Enter Car ID to Remove: ");
                        int removeCarId = sc.nextInt();
                        removeNumber(connection, removeCarId);
                        break;
                    case 11:
                        System.out.print("Enter Driver ID to Remove: ");
                        int removeDriverId = sc.nextInt();
                        removeDriver(connection, removeDriverId);
                        break;
                    case 12:
                        System.out.print("Enter Car ID to Remove Fines: ");
                        int removeFinesCarId = sc.nextInt();
                        removeFines(connection, removeFinesCarId);
                        break;
                    case 13:
                        System.out.print("Enter Experience: ");
                        int exp = sc.nextInt();
                        getExperiencedDrivers(connection, exp);
                        break;
                    case 14:
                        System.out.println("Enter type: ");
                        String typ = sc.nextLine();
                        getTypeFines(connection, typ);
                        break;
                    case 0:
                        System.out.println("Exiting...");
                        return;
                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
            }
        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных) возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            if (e.getSQLState().startsWith("23")) {
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }



    // endregion

    // region // SELECT-запросы без параметров в одной таблице

    private static void getWithNumber(Connection connection) throws SQLException {
        // имена столбцов
        String columnId = "id", columnNumber = "car_number", columnBrand = "brand", columnModel = "model", columnFines = "Fines", columnOwner = "owner";
        // значения ячеек
        int param0 = -1;
        String param1 = null, param2 = null, param3 = null, param5 = null, param4 = null;
        /* boolean param4;*/

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM car ORDER BY 1;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {
            param0 = rs.getInt(columnId);
            param1 = rs.getString(columnNumber);
            param2 = rs.getString(columnBrand);
            param3 = rs.getString(columnModel);
            param4 = rs.getString(columnFines);
            param5 = rs.getString(columnOwner);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5);
        }
    }

    static void getDriver(Connection connection) throws SQLException {
        int param0 = -1;
        String param1 = null, param2 = null, param3 = null, param4 = null, param5 = null, param6 = null, param7 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM driver ORDER BY 1;");  // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getString(4);
            param4 = rs.getString(5);
            param5 = rs.getString(6);
            param6 = rs.getString(7);
            param7 = rs.getString(8);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6 + " | " + param7);
        }
    }

    static void getFines(Connection connection) throws SQLException {
        int param0 = -1, param3 = -1;
        String param1 = null, param2 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM fines ORDER BY 1;");  // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {
            param0 = rs.getInt(2);
            param1 = rs.getString(3);
            param2 = rs.getString(4);
            param3 = rs.getInt(5);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }
    }



    private static void getFinesCar(Connection connection, int car_id) throws SQLException {
        if (car_id <= 0) return;

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT car.id, car.car_number, car.brand, car.model, car.fines, car.owner " +
                        "FROM fines " +
                        "JOIN car ON fines.car_id = car.id " +
                        "WHERE fines.car_id = ?;"
        );
        statement.setInt(1, car_id);

        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println("Car ID: " + rs.getInt(1) + " | Number: " + rs.getString(2) + " | Brand: " + rs.getString(3) + " | Model: " + rs.getString(4) + " | Fines: " + rs.getString(5) + " | Owner: " + rs.getString(6));
        }

        rs.close();
        statement.close();

        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " ms.)");
    }

    private static void getCarDriver(Connection connection, int id) throws SQLException {
        if (id <= 0) return;

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                    "SELECT car.id, car.car_number, car.brand, car.model, car.fines, car.owner " +
                            "FROM driver " +
                            "JOIN car ON driver.driver_id = car.id " +
                            "WHERE driver.driver_id = ?;"
        );
        statement.setInt(1, id);

        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println("Car ID: " + rs.getInt(1) + " | Number: " + rs.getString(2) + " | Brand: " + rs.getString(3) + " | Model: " + rs.getString(4) + " | Fines: " + rs.getString(5) + " | Owner: " + rs.getString(6));
        }

        rs.close();
        statement.close();

        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " ms.)");
    }


    private static void addNumber(Connection connection, String number, String brand, String model, String fines, String owner) throws SQLException {
        if (number == null || number.isBlank() || brand.isBlank() || model.isBlank() || fines == null || owner.isBlank()) {
            System.out.println("Invalid parameters!");
            return;
        }

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO car(car_number, brand, model, fines, owner) VALUES (?, ?, ?, ?, ?);",
                Statement.RETURN_GENERATED_KEYS
        );
        statement.setString(1, number);
        statement.setString(2, brand);
        statement.setString(3, model);
        statement.setString(4, fines);
        statement.setString(5, owner);

        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("Идентификатор машины " + rs.getInt(1) + " Номер машины " + rs.getString(2) + " Марка " + rs.getString(3) + " Модель " + rs.getString(4) + " Штрафы " + rs.getString(5) + " Владелец " + rs.getString(6));
        }

        System.out.println("INSERTed " + count + " car");
        getWithNumber(connection);
    }

    private static void addDriver(Connection connection, String first_name, String last_name, String patronymic, int age, int experience, int license_number, int driver_id) throws SQLException {
        if (first_name == null || first_name.isBlank() || last_name == null || last_name.isBlank() || patronymic == null || patronymic.isBlank() || license_number <= 0 || driver_id <= 0 || age <= 0 || experience <= 0) {
            System.out.println("Invalid parameters!");
            return;
        }

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO driver(first_name, last_name , patronymic, age, experience, license_number, driver_id) VALUES (?, ?, ?, ?, ?, ?, ?) returning id;",
                Statement.RETURN_GENERATED_KEYS
        );

        statement.setString(1, first_name);
        statement.setString(2, last_name);
        statement.setString(3, patronymic);
        statement.setInt(4, age);
        statement.setInt(5, experience);
        statement.setInt(6, license_number);
        statement.setInt(7, driver_id);

        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            int generatedId = rs.getInt(1);
            System.out.println("Generated ID: " + generatedId);
        }

        System.out.println("INSERTed " + count + " driver");
        getDriver(connection);
    }

    private static void addFines(Connection connection, int car_id, String car_number, String type, int cost) throws SQLException {
        if (car_id <= 0 || car_number == null || car_number.isBlank() || type == null || type.isBlank() || cost <= 0) {
            System.out.println("Invalid parameters!");
            return;
        }

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO fines(car_id, car_number, type, cost) VALUES (?, ?, ?, ?) returning penalty_number;",
                Statement.RETURN_GENERATED_KEYS
        );

        statement.setInt(1, car_id);
        statement.setString(2, car_number);
        statement.setString(3, type);
        statement.setInt(4, cost);
        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            int penaltyNumber = rs.getInt(1);
            System.out.println("Generated Penalty Number: " + penaltyNumber);
        }

        rs.close();
        statement.close();

        System.out.println("INSERTed " + count + " fine");
        // getDriver(connection);
    }

    private static void correctCar(Connection connection, int id, String fines) throws SQLException {
        if (fines == null || fines.isBlank() || id <= 0) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE car SET fines=? WHERE id=?;");
        statement.setString(1, fines);
        statement.setInt(2, id);
        statement.executeUpdate();
        System.out.println("Successful");
        getWithNumber(connection);
    }

    private static void removeNumber(Connection connection, int id) throws SQLException {
        if (id <= 0) return;
        try (PreparedStatement statement = connection.prepareStatement("DELETE from driver WHERE id=?;")) {
            statement.setInt(1, id);
            ResultSet rs = statement.executeQuery();
        } catch (Exception e) {

        }

        PreparedStatement statement = connection.prepareStatement("DELETE from car WHERE id=?;");
        statement.setInt(1, id);

        int count = statement.executeUpdate(); // выполняем запрос на удаление и возвращаем количество измененных строк
        System.out.println("DELETEd " + count);
        getWithNumber(connection);
    }

    private static void removeDriver(Connection connection, int driver_id) throws SQLException {
        if (driver_id <= 0) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from driver WHERE id=?;");
        statement.setInt(1, driver_id);

        int count = statement.executeUpdate();
        System.out.println("DELETEd " + count);
        getDriver(connection);
    }

    private static void removeFines(Connection connection, int car_id) throws SQLException {
        if (car_id <= 0) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from fines WHERE car_id=?;");
        statement.setInt(1, car_id);

        int count = statement.executeUpdate();
        System.out.println("DELETEd " + count);
        getDriver(connection);
    }

    private static void getExperiencedDrivers(Connection connection, int experience) throws SQLException {
        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT id, first_name, last_name, patronymic, age, experience, license_number, driver_id " +
                        "FROM driver " +
                        "WHERE experience > ? " +
                        "ORDER BY experience;"
        );
        statement.setInt(1, experience);

        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println("Driver ID: " + rs.getInt(1) + " | First Name: " + rs.getString(2) + " | Last Name: " + rs.getString(3) + " | Patronymic: " + rs.getString(4) + " | Age: " + rs.getInt(5) + " | Experience: " + rs.getInt(6) + " | License Number: " + rs.getInt(7) + " | Driver ID: " + rs.getInt(8));
        }

        rs.close();
        statement.close();

        System.out.println("SELECT with WHERE and ORDER BY (" + (System.currentTimeMillis() - time) + " ms.)");
    }
    static void getTypeFines(Connection connection, String type) throws SQLException {
        if(type == null || type.isBlank()) return;
        type = '%' + type + '%';
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM fines WHERE type LIKE ?;");
        statement.setString(1, type);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getInt(1) + "|" + rs.getInt(2) + "|" + rs.getString(3)  + "|" + rs.getString(4)  + "|" + rs.getInt(5));
        }
    }

}
