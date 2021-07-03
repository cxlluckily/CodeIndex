package me.iroohom.pattern.builder.after;

/**
 * 抽象的建造者
 */
public abstract class HouseBuilder {
    protected House house = new House();

    //建造的流程
    public abstract void buildBase();

    public abstract void buildWall();

    public abstract void buildRoof();

    //建造好房子之后，将建造好的房子返回
    public House buildHouse() {
        return house;
    }


}
