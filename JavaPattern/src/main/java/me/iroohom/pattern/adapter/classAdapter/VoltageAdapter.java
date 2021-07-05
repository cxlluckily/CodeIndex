package me.iroohom.pattern.adapter.classAdapter;

public class VoltageAdapter extends Voltage220V implements IVoltage5V {
    @Override
    public int output5V() {
        int scrV = output220V();
        return scrV / 44;
    }
}
