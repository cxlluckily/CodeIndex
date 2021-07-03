package me.iroohom.pattern.builder.after;

public class HouseDirector {
    HouseBuilder houseBuilder = null;

    //方式一: 通过构造器传入houseBuilder
    public HouseDirector(HouseBuilder houseBuilder) {
        this.houseBuilder = houseBuilder;

    }

    //方式二 通过setter
    public void setHouseBuilder(HouseBuilder houseBuilder) {
        this.houseBuilder = houseBuilder;
    }

    //建房子的具体流程，交给导演类来做
    public House constructHouse() {
        houseBuilder.buildBase();
        houseBuilder.buildWall();
        houseBuilder.buildRoof();
        return houseBuilder.buildHouse();
    }
}
