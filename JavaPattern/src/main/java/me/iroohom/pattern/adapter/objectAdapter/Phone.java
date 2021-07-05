package me.iroohom.pattern.adapter.objectAdapter;

public class Phone {
    public void charging(IVoltage5V iVoltage5V) {
        int transformVol = iVoltage5V.output5V();

        if (transformVol == 5) {
            System.out.println("可以充电");
        } else if (transformVol != 5) {
            System.out.println("不可以充电");
        }
    }
}
