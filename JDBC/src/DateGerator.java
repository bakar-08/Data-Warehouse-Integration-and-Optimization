import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
public class DateGerator {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/ELECTRONICA-DW"; // Update with your MySQL URL
        String username = "root"; // Update with your MySQL username
        String password = "HelloWorld123!"; // Update with your MySQL password

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            // Prepare SQL statement for inserting data into the date dimension table
            String insertQuery = "INSERT INTO date_dimension (dates, year, month, day, day_of_week, day_of_month, day_of_year) VALUES (?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

            // Iterate through each day of the year 2019
            LocalDate startDate = LocalDate.of(2019, 1, 1);
            LocalDate endDate = LocalDate.of(2019, 12, 31);

            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            while (!startDate.isAfter(endDate)) {
                // Extract date-related information
                int year = startDate.getYear();
                int month = startDate.getMonthValue();
                int day = startDate.getDayOfMonth();
                String dayOfWeek = startDate.getDayOfWeek().toString();
                int dayOfMonth = day;
                int dayOfYear = startDate.getDayOfYear();

                // Insert data into the date dimension table
                preparedStatement.setString(1, startDate.format(dateFormatter));
                preparedStatement.setInt(2, year);
                preparedStatement.setInt(3, month);
                preparedStatement.setInt(4, day);
                preparedStatement.setString(5, dayOfWeek);
                preparedStatement.setInt(6, dayOfMonth);
                preparedStatement.setInt(7, dayOfYear);

                // Execute the insert query
                preparedStatement.executeUpdate();

                // Move to the next day
                startDate = startDate.plusDays(1);
            }
            System.out.println("Date Dimension Table populated successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

