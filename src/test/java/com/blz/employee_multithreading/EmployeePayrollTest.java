package com.blz.employee_multithreading;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.blz.employee_multithreading.EmployeePayrollService.IOService;

public class EmployeePayrollTest {

	private static EmployeePayrollService employeePayrollService;

	@BeforeClass
	public static void createEmployeePayrollObj() {
		employeePayrollService = new EmployeePayrollService();
		System.out.println("Welcome to the Employee Payroll Program.. ");
	}

	@Test
	public void given6Employees_WhenAddedDataToDB_ShouldMatchEmployeesEnteries()
			throws EmployeePayrollException, SQLException {
		EmployeePayrollData[] arrayOfEmps = { new EmployeePayrollData(0, "Anil", "M", 1000000, LocalDate.now()),
				new EmployeePayrollData(0, "Balu", "M", 2000000, LocalDate.now()),
				new EmployeePayrollData(0, "Anjani", "F", 3000000, LocalDate.now()),
				new EmployeePayrollData(0, "Sunil", "M", 4000000, LocalDate.now()),
				new EmployeePayrollData(0, "Pooja", "F", 5000000, LocalDate.now()),
				new EmployeePayrollData(0, "Bantu", "M", 6000000, LocalDate.now()), };

		employeePayrollService.readEmployeePayrollData(IOService.DB_IO);
		Instant start = Instant.now();
		employeePayrollService.addEmployeePayrollData_MultiThread(Arrays.asList(arrayOfEmps));
		Instant end = Instant.now();
		System.out.println("Duration without thread: " + Duration.between(start, end));
		Instant threadStart = Instant.now();
		employeePayrollService.addEmployeeToPayRollWIthThreads(Arrays.asList(arrayOfEmps));
		Instant threadEnd = Instant.now();
		System.out.println("Duration With Thread : " + Duration.between(threadStart, threadEnd));
		employeePayrollService.printData(IOService.DB_IO);
		Assert.assertEquals(15, employeePayrollService.countEnteries(IOService.DB_IO));
	}
}
