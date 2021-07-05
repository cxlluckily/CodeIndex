package me.iroohom.pattern.adapter.objectAdapter;

public class VoltageAdapter implements IVoltage5V {
    private Voltage220V voltage220V;

    /**
     * 通过构造器传入一个voltage220V实例
     *
     * @param voltage220V
     */
    public VoltageAdapter(Voltage220V voltage220V) {
        this.voltage220V = voltage220V;
    }

    @Override
    public int output5V() {
        int dst = 0;

        if (null != voltage220V) {
            int src = voltage220V.output220V();
            System.out.println("使用对象适配器进行适配...");
            dst = src / 44;
            System.out.println("适配完成的电压为:" + dst + "V");
        }

        return dst;
    }
}
