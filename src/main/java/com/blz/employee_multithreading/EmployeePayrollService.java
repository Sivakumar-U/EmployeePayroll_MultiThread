package com.blz.employee_multithreading;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EmployeePayrollService {

	public enum IOService {
		DB_IO, FILE_IO
	}

	private List<EmployeePayrollData> employeePayrollList;
	private static EmployeePayrollDBService employeePayrollDBService;

	public EmployeePayrollService() {
		employeePayrollDBService = EmployeePayrollDBService.getInstance();
	}

	public List<EmployeePayrollData> readEmployeePayrollData(IOService ioService) {
		if (ioService.equals(IOService.DB_IO))
			this.employeePayrollList = employeePayrollDBService.readData();
		return this.employeePayrollList;
	}

	public void updateRecord(String name, double salary) throws EmployeePayrollException {
		int result = employeePayrollDBService.updateEmployeeData(name, salary);
		if (result == 0)
			return;
		EmployeePayrollData employeePayrollData = this.getEmployeePayrollData(name);
		if (employeePayrollData != null)
			employeePayrollData.salary = salary;
	}

	public boolean checkUpdatedRecordSyncWithDatabase(String name) throws EmployeePayrollException {
		List<EmployeePayrollData> employeePayrollDataList = employeePayrollDBService.getEmployeePayRollData(name);
		return employeePayrollDataList.get(0).equals(getEmployeePayrollData(name));
	}

	private EmployeePayrollData getEmployeePayrollData(String name) {
		return this.employeePayrollList.stream()
				.filter(employeePayrollDataItem -> employeePayrollDataItem.name.equals(name)).findFirst().orElse(null);
	}

	public List<EmployeePayrollData> readPayRollDataForDateRange(IOService ioService, LocalDate startDate,
			LocalDate endDate) {
		if (ioService.equals(IOService.DB_IO)) {
			return employeePayrollDBService.getEmployeePayRollForDateRange(startDate, endDate);
		}
		return null;
	}

	public Map<String, Double> readAverageSalaryByGender(IOService ioService) throws SQLException {
		if (ioService.equals(IOService.DB_IO))
			return employeePayrollDBService.getAverageSalaryByGender();
		return null;
	}

	public void addEmployeePayRollData(String name, String gender, double salary, LocalDate date) throws SQLException {
		employeePayrollList.add(employeePayrollDBService.addEmployeeToPayRoll(name, gender, salary, date));

	}

	public void addEmployeePayrollData(String name, String gender, double salary, LocalDate date, String companyName,
			int companyId, String department) {

		employeePayrollList.add(employeePayrollDBService.addEmployeeToPayRoll(name, gender, salary, date, companyName,
				companyId, department));
	}

	public void addEmployeePayrollData_MultiThread(List<EmployeePayrollData> employeePayrollList) {
		employeePayrollList.forEach(employeePayrollData -> {
			try {
				this.addEmployeePayRollData(employeePayrollData.name, employeePayrollData.gender,
						employeePayrollData.salary, employeePayrollData.startDate);

			} catch (SQLException e) {

				e.printStackTrace();
			}

		});

	}

	public long countEnteries(IOService ioService) {
		if (ioService.equals(IOService.FILE_IO))
			return new EmployeePayrollService().countEnteries(ioService);
		return employeePayrollList.size();
	}

	public void addEmployeeToPayRollWIthThreads(List<EmployeePayrollData> employeePayRollList) {
		Map<Integer, Boolean> employeeAditionStatus = new HashMap<Integer, Boolean>();
		employeePayRollList.forEach(employeePayRollData -> {
			Runnable task = () -> {
				employeeAditionStatus.put(employeePayRollData.hashCode(), false);
				System.out.println("Employee Added:" + Thread.currentThread().getName());
				try {
					this.addEmployeePayRollData(employeePayRollData.name, employeePayRollData.gender,
							employeePayRollData.salary, employeePayRollData.startDate);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				employeeAditionStatus.put(employeePayRollData.hashCode(), true);
				System.out.println("Employee Added: " + Thread.currentThread().getName());

			};
			Thread thread = new Thread(task, employeePayRollData.name);
			thread.start();
		});
		while (employeeAditionStatus.containsValue(false)) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
			}
		}

	}

	public void printData(IOService ioService) {
		if (ioService.equals(IOService.FILE_IO))
			new EmployeePayrollService().printData(ioService.DB_IO);
		else
			System.out.println(employeePayrollList);

	}

}
