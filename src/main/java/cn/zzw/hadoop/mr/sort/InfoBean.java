package cn.zzw.hadoop.mr.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class InfoBean implements WritableComparable<InfoBean> {

	private String account;	
	private double incom;
	private double expenses;
	private double surplus;
	
	public void set(String account, double income, double expenses) {
		this.account = account;
		this.incom = income;
		this.expenses = expenses;
		this.surplus = income - expenses;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.account = in.readUTF();
		this.incom = in.readDouble();
		this.expenses = in.readDouble();
		this.surplus = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(account);
		out.writeDouble(incom);
		out.writeDouble(expenses);
		out.writeDouble(surplus);
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public double getIncom() {
		return incom;
	}

	public void setIncom(double incom) {
		this.incom = incom;
	}

	public double getExpenses() {
		return expenses;
	}

	public void setExpenses(double expenses) {
		this.expenses = expenses;
	}

	public double getSurplus() {
		return surplus;
	}

	public void setSurplus(double surplus) {
		this.surplus = surplus;
	}

	public int compareTo(InfoBean o) {
		if (this.incom == o.getIncom()) {
			return this.expenses > o.getExpenses() ? 1 : -1;
		} else {
			return this.incom > o.getIncom() ? -1 : 1;
		}
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.incom + "\t" + this.expenses + "\t" + this.surplus;
	}

}
