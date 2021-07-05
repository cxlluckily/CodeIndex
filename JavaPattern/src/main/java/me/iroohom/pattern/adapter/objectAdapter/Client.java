package me.iroohom.pattern.adapter.objectAdapter;

public class Client {
    public static void main(String[] args) {

        Phone phone = new Phone();
        phone.charging(new VoltageAdapter(new Voltage220V()));
    }
}
