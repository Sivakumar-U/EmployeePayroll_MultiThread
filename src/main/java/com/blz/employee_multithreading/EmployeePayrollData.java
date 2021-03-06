package com.blz.employee_multithreading;

import java.time.LocalDate;

public class EmployeePayrollData {

	public int id;
	public String name;
	public double salary;
	public LocalDate startDate;
	public String companyName;
	public int companyId;
	public String department;
	public String gender;

	public EmployeePayrollData(int id, String name, double salary) {
		super();
		this.id = id;
		this.name = name;
		this.salary = salary;
	}
	public EmployeePayrollData(int id, String name, double salary, LocalDate startDate) {
		super();
		this.id = id;
		this.name = name;
		this.salary = salary;
		this.startDate = startDate;
	}
	
	public EmployeePayrollData(int id, String name, double salary, LocalDate startDate, String companyName,
			int companyId, String department) {
		super();
		this.id = id;
		this.name = name;
		this.salary = salary;
		this.startDate = startDate;
		this.companyName = companyName;
		this.companyId = companyId;
		this.department = department;
	}
	public EmployeePayrollData(int id, String name, String gender, double salary, LocalDate startDate) {
		this(id,name,salary,startDate);
		this.gender = gender;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EmployeePayrollData other = (EmployeePayrollData) obj;
		if (gender == null) {
			if (other.gender != null)
				return false;
		} else if (!gender.equals(other.gender))
			return false;
		if (id != other.id)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (Double.doubleToLongBits(salary) != Double.doubleToLongBits(other.salary))
			return false;
		if (startDate == null) {
			if (other.startDate != null)
				return false;
		} else if (!startDate.equals(other.startDate))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "EmployeePayrollData [id=" + id + ", name=" + name + ", salary=" + salary + ", start=" + startDate + "]";
	}

}
