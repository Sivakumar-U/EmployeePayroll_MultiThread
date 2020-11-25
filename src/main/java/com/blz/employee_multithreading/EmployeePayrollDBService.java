package com.blz.employee_multithreading;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.blz.employee_multithreading.EmployeePayrollException.ExceptionType;

public class EmployeePayrollDBService {

	private PreparedStatement employeePayrollPreparedStatement;
	private static EmployeePayrollDBService employeePayrollDBService;

	private EmployeePayrollDBService() {
	}

	public static EmployeePayrollDBService getInstance() {
		if (employeePayrollDBService == null)
			employeePayrollDBService = new EmployeePayrollDBService();
		return employeePayrollDBService;
	}

	private Connection getConnection() throws SQLException {
		String jdbcURL = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
		String username = "root";
		String password = "Amigos@1";
		Connection con;
		System.out.println("Connecting to database:" + jdbcURL);
		con = DriverManager.getConnection(jdbcURL, username, password);
		System.out.println("Connection is successful:" + con);
		return con;
	}

	public List<EmployeePayrollData> readData() {
		String sql = "SELECT* FROM employee_payroll;";
		return this.getEmployeePayRollDataUsingDB(sql);
	}

	public List<EmployeePayrollData> getEmployeePayRollData(String name) {
		List<EmployeePayrollData> employeePayRollList = null;
		if (this.employeePayrollPreparedStatement == null)
			this.prepareStatementForEmployeeData();
		try {
			employeePayrollPreparedStatement.setString(1, name);
			ResultSet resultSet = employeePayrollPreparedStatement.executeQuery();
			employeePayRollList = this.getEmployeePayRollData(resultSet);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return employeePayRollList;
	}

	private List<EmployeePayrollData> getEmployeePayRollData(ResultSet resultSet) {
		List<EmployeePayrollData> employeePayrollList = new ArrayList<EmployeePayrollData>();
		try {
			while (resultSet.next()) {
				int id = resultSet.getInt("id");
				String name = resultSet.getString("name");
				double salary = resultSet.getDouble("salary");
				LocalDate startDate = resultSet.getDate("start").toLocalDate();
				employeePayrollList.add(new EmployeePayrollData(id, name, salary, startDate));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return employeePayrollList;
	}

	private void prepareStatementForEmployeeData() {
		try {
			Connection connection = this.getConnection();
			String sql = "SELECt * FROM employee_payroll WHERE name = ?";
			employeePayrollPreparedStatement = connection.prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public int updateEmployeeData(String name, double salary) throws EmployeePayrollException {
		return this.updateEmployeeDataUsingStatement(name, salary);
	}

	public int updateEmployeeDataUsingStatement(String name, double salary) throws EmployeePayrollException {
		String sql = String.format("UPDATE employee_payroll set salary=%.2d where name=%s", name, salary);
		try (Connection connection = this.getConnection();) {
			Statement statement = connection.createStatement();
			return (statement.executeUpdate(sql));
		} catch (SQLException e) {
			throw new EmployeePayrollException("Wrong SQL query given", ExceptionType.WRONG_SQL);
		}
	}

	public int updateSalaryUsingSQL(String name, Double salary) throws SQLException {
		String sql = "UPDATE employee_payroll SET salary=? WHERE name=?";
		try (Connection connection = getConnection()) {
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setDouble(1, salary);
			preparedStatement.setString(2, name);
			return preparedStatement.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public List<EmployeePayrollData> getEmployeePayRollForDateRange(LocalDate startDate, LocalDate endDate) {
		String sql = String.format("SELECT * FROM employee_payroll WHERE START BETWEEN '%s' AND '%s' ;",
				Date.valueOf(startDate), Date.valueOf(endDate));
		return this.getEmployeePayRollDataUsingDB(sql);
	}

	private List<EmployeePayrollData> getEmployeePayRollDataUsingDB(String sql) {
		List<EmployeePayrollData> employeePayrollList = new ArrayList<EmployeePayrollData>();
		try (Connection connection = this.getConnection();) {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			employeePayrollList = this.getEmployeePayRollData(resultSet);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return employeePayrollList;
	}

	public Map<String, Double> getAverageSalaryByGender() throws SQLException {
		String sql = "SELECT GENDER,AVG(SALARY) AS AVG_SALARY FROM employee_payroll GROUP BY GENDER;";
		Map<String, Double> genderToAverageSalaryMap = new HashMap<>();
		try (Connection connection = this.getConnection()) {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				String gender = resultSet.getString("GENDER");
				double salary = resultSet.getDouble("AVG_SALARY");
				genderToAverageSalaryMap.put(gender, salary);
			}
		}
		return genderToAverageSalaryMap;
	}

	public EmployeePayrollData addEmployeeToPayRoll(String name, String gender, double salary, LocalDate date)
			throws SQLException {
		int id = -1;
		Connection connection = null;
		EmployeePayrollData employeePayRollData = null;
		try {
			connection = this.getConnection();
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try (Statement statement = connection.createStatement();) {
			String sql = String.format(
					"INSERT INTO employee_payroll (name, gender, salary, start)" + "VALUES ( '%s', '%s', '%s', '%s')",
					name, gender, salary, Date.valueOf(date));
			int rowAffected = statement.executeUpdate(sql, statement.RETURN_GENERATED_KEYS);
			if (rowAffected == 1) {
				ResultSet resultSet = statement.getGeneratedKeys();
				if (resultSet.next())
					id = resultSet.getInt(1);
			}
			employeePayRollData = new EmployeePayrollData(id, name, salary, date);
		} catch (SQLException e) {
			e.printStackTrace();
			connection.rollback();
		}
		try (Statement statement = connection.createStatement();) {
			double deductions = salary * 0.2;
			double taxablePay = salary - deductions;
			double tax = taxablePay * 0.1;
			double netPay = salary - tax;
			String sql = String
					.format("INSERT INTO payroll_details (employee_id, basic_pay, deductions, taxable_pay,tax,net_pay)"
							+ "VALUES ( %s, %s, %s, %s, %s, %s)", id, salary, deductions, taxablePay, tax, tax, netPay);
			int rowAffected = statement.executeUpdate(sql);
			if (rowAffected == 1) {
				employeePayRollData = new EmployeePayrollData(id, name, salary, date);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			connection.rollback();
		}
		try {
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null)
				connection.close();
		}

		return employeePayRollData;

	}

	public EmployeePayrollData addEmployeeToPayRoll(String name, String gender, double salary, LocalDate date,
			String companyName, int companyId, String department) {
		int id = -1;
		Connection connection = null;
		EmployeePayrollData employeePayRollData = null;
		try {
			connection = this.getConnection();
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try (Statement statement = connection.createStatement();) {
			String sql = String.format(
					"INSERT INTO employee_payroll (name, gender, salary, start)" + "VALUES ( '%s', '%s', '%s', '%s')",
					name, gender, salary, Date.valueOf(date));
			int rowAffected = statement.executeUpdate(sql, statement.RETURN_GENERATED_KEYS);
			if (rowAffected == 1) {
				ResultSet resultSet = statement.getGeneratedKeys();
				if (resultSet.next())
					id = resultSet.getInt(1);
			}
			employeePayRollData = new EmployeePayrollData(id, name, salary, date);
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				connection.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		try (Statement statement = connection.createStatement();) {
			double deductions = salary * 0.2;
			double taxablePay = salary - deductions;
			double tax = taxablePay * 0.1;
			double netPay = salary - tax;
			String sql = String
					.format("INSERT INTO payroll_details (employee_id, basic_pay, deductions, taxable_pay,tax,net_pay)"
							+ "VALUES ( %s, %s, %s, %s, %s, %s)", id, salary, deductions, taxablePay, tax, tax, netPay);
			int rowAffected = statement.executeUpdate(sql);
			if (rowAffected == 1) {
				employeePayRollData = new EmployeePayrollData(id, name, salary, date);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			try {
				connection.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		try (Statement statement = connection.createStatement();) {
			String department_name = department;
			String sql = String.format("INSERT INTO department(EMPLOYEE_ID,DEPARTMENT_NAME)" + "VALUES ( %s, %s)", id,
					department);
			int rowAffected = statement.executeUpdate(sql);
			if (rowAffected == 1) {
				employeePayRollData = new EmployeePayrollData(id, name, salary, date);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			try {
				connection.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		try (Statement statement = connection.createStatement();) {
			String company_Name = companyName;
			int company_id = companyId;
			String sql = String.format(
					"INSERT INTO company_details (company_id,company_name,EMPLOYEE_ID )" + "VALUES ( %s, %s, %s)",
					company_id, company_Name, id);
			int rowAffected = statement.executeUpdate(sql);
			if (rowAffected == 1) {
				employeePayRollData = new EmployeePayrollData(id, name, salary, date);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			try {
				connection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		try {
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null)
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}

		return employeePayRollData;

	}

}
